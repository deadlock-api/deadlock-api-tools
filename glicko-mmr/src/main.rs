#![forbid(unsafe_code)]
#![deny(clippy::all)]
#![deny(unreachable_pub)]
#![deny(clippy::pedantic)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::implicit_hasher)]

use std::collections::HashMap;

use clap::Parser;
use tracing::info;

use crate::config::Config;
use crate::types::{CHMatch, Glicko2HistoryEntry};

pub mod config;
pub mod glicko;
pub mod types;
pub mod utils;

const UPDATE_INTERVAL: u64 = 30 * 60; // 30 minutes

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    common::init_tracing();
    common::init_metrics()?;

    let ch_client = common::get_ch_client()?;
    let config = Config::parse();

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(UPDATE_INTERVAL));
    loop {
        let Ok(start_match_id) = utils::get_start_match_id(&ch_client).await else {
            info!("No matches to process, sleeping...");
            interval.tick().await;
            continue;
        };
        info!("Processing matches starting from {start_match_id}");
        let matches_to_process =
            CHMatch::query_matches_after(&ch_client, start_match_id, 100_000).await?;
        let num_matches = matches_to_process.len();
        if num_matches == 0 {
            info!("No matches to process, sleeping...");
            interval.tick().await;
            continue;
        }
        let mut player_ratings_before =
            Glicko2HistoryEntry::query_before_match_id(&ch_client, start_match_id)
                .await?
                .into_iter()
                .map(|entry| (entry.account_id, entry))
                .collect::<HashMap<_, _>>();

        let mut squared_error = 0.0;
        let mut inserter = ch_client.insert("glicko")?;
        for match_ in matches_to_process {
            let updates: Vec<(Glicko2HistoryEntry, f64)> =
                glicko::update_match(&config, &match_, &player_ratings_before);
            for (update, error) in updates {
                squared_error += error * error;
                inserter.write(&update).await?;
                player_ratings_before.insert(update.account_id, update);
            }
        }
        inserter.end().await?;
        info!(
            "{num_matches} Matches processed, Avg Error: {}",
            (squared_error / 12. / num_matches as f64).sqrt()
        );
    }
}
