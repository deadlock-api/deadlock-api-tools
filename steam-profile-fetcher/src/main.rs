use anyhow::Result;
use arl::RateLimiter;
use itertools::Itertools;
use metrics::{counter, gauge};
use once_cell::sync::Lazy;
use rand::rng;
use rand::seq::SliceRandom;
use sqlx::postgres::PgQueryResult;
use sqlx::{Error, PgPool};
use std::env;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, instrument};
mod models;
mod steam_api;

use models::SteamPlayerSummary;

static FETCH_INTERVAL: Lazy<Duration> = Lazy::new(|| {
    Duration::from_secs(
        env::var("FETCH_INTERVAL_SECONDS")
            .unwrap_or_else(|_| "600".to_string())
            .parse()
            .unwrap_or(10 * 60),
    )
});

static REQUESTS_PER_10_MINUTES: Lazy<usize> = Lazy::new(|| {
    env::var("REQUESTS_PER_10_MINUTES")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap_or(10)
});

static BATCH_SIZE: Lazy<usize> = Lazy::new(|| {
    env::var("BATCH_SIZE")
        .unwrap_or_else(|_| "100".to_string())
        .parse()
        .unwrap_or(100)
        .min(100) // Steam API has a limit of 100 accounts per request
});

#[tokio::main]
async fn main() -> Result<()> {
    common::init_tracing();
    common::init_metrics()?;

    info!("Starting Steam Profile Fetcher");

    let http_client = reqwest::Client::new();
    let ch_client = common::get_ch_client()?;
    let pg_client = common::get_pg_client().await?;

    let limiter = RateLimiter::new(*REQUESTS_PER_10_MINUTES, Duration::from_secs(10 * 60));
    let mut interval = tokio::time::interval(*FETCH_INTERVAL);
    loop {
        interval.tick().await;
        match fetch_and_update_profiles(&http_client, &ch_client, &pg_client, &limiter).await {
            Ok(_) => info!("Updated Steam profiles"),
            Err(e) => {
                error!("Error updating Steam profiles: {}", e);
                continue;
            }
        }
    }
}

#[instrument(skip_all)]
async fn fetch_and_update_profiles(
    http_client: &reqwest::Client,
    ch_client: &clickhouse::Client,
    pg_client: &PgPool,
    limiter: &RateLimiter,
) -> Result<()> {
    let account_ids = get_account_ids_to_update(ch_client, pg_client).await?;

    if account_ids.is_empty() {
        info!("No new account IDs to update, sleeping 10min...");
        gauge!("steam_profile_fetcher.account_ids_to_update").set(0);
        sleep(Duration::from_secs(60 * 10)).await;
        return Ok(());
    }

    info!("Found {} account IDs to update", account_ids.len());
    gauge!("steam_profile_fetcher.account_ids_to_update").set(account_ids.len() as f64);

    let mut remaining = account_ids.len();
    limiter.wait().await;

    let profiles = match steam_api::fetch_steam_profiles(http_client, &account_ids).await {
        Ok(profiles) => {
            info!("Fetched {} Steam profiles", profiles.len());
            counter!("steam_profile_fetcher.fetched_profiles.success")
                .increment(profiles.len() as u64);
            profiles
        }
        Err(e) => {
            error!("Failed to fetch Steam profiles: {}", e);
            counter!("steam_profile_fetcher.fetched_profiles.failure")
                .increment(account_ids.len() as u64);
            return Err(e);
        }
    };

    match save_profiles(pg_client, &profiles).await {
        Ok(_) => {
            remaining -= profiles.len();
            info!(
                "Saved {} Steam profiles, {} account IDs remaining to update",
                profiles.len(),
                remaining
            );
            gauge!("steam_profile_fetcher.account_ids_to_update").decrement(profiles.len() as f64);
            counter!("steam_profile_fetcher.saved_profiles.success")
                .increment(profiles.len() as u64);
        }
        Err(e) => {
            error!("Failed to save Steam profiles: {}", e);
            counter!("steam_profile_fetcher.saved_profiles.failure")
                .increment(profiles.len() as u64);
            return Err(e.into());
        }
    }

    Ok(())
}

async fn get_account_ids_to_update(
    ch_client: &clickhouse::Client,
    pg_client: &PgPool,
) -> Result<Vec<u32>> {
    let ch_account_ids = get_ch_account_ids(ch_client).await?;
    let mut pg_account_ids = get_pg_account_ids(pg_client).await?;
    pg_account_ids.shuffle(&mut rng());

    Ok(ch_account_ids
        .into_iter()
        .chain(pg_account_ids.into_iter())
        .unique()
        .take(*BATCH_SIZE)
        .collect())
}

async fn get_ch_account_ids(ch_client: &clickhouse::Client) -> clickhouse::error::Result<Vec<u32>> {
    let query = "
SELECT DISTINCT account_id
FROM match_player
WHERE match_id IN (SELECT match_id FROM match_info WHERE start_time > now() - INTERVAL 1 MONTH)
AND account_id > 0
ORDER BY RAND()
    ";
    ch_client.query(query).fetch_all().await
}

async fn get_pg_account_ids(pg_client: &PgPool) -> sqlx::Result<Vec<u32>> {
    Ok(
        sqlx::query!("SELECT DISTINCT account_id FROM steam_profiles WHERE last_updated > now() - INTERVAL '2 weeks'")
            .fetch_all(pg_client)
            .await?
            .into_iter()
            .map(|row| row.account_id as u32)
            .collect(),
    )
}

#[instrument(skip_all)]
async fn save_profiles(
    pg_client: &PgPool,
    profiles: &[SteamPlayerSummary],
) -> std::result::Result<PgQueryResult, Error> {
    let mut query_builder = sqlx::QueryBuilder::new(
        "INSERT INTO steam_profiles (
            account_id, personaname, profileurl,
            avatar, personastate, realname, countrycode
        ) ",
    );

    query_builder.push_values(profiles, |mut b, profile| {
        b.push_bind(common::steam_id64_to_account_id(profile.steamid.parse().unwrap()) as i32)
            .push_bind(&profile.personaname)
            .push_bind(&profile.profileurl)
            .push_bind(&profile.avatar)
            .push_bind(profile.personastate as i32)
            .push_bind(&profile.realname)
            .push_bind(&profile.loccountrycode);
    });

    query_builder.push(
        " ON CONFLICT (account_id)
        DO UPDATE SET
            personaname = EXCLUDED.personaname,
            profileurl = EXCLUDED.profileurl,
            avatar = EXCLUDED.avatar,
            personastate = EXCLUDED.personastate,
            realname = EXCLUDED.realname,
            countrycode = EXCLUDED.countrycode",
    );

    let query = query_builder.build();
    query.execute(pg_client).await
}
