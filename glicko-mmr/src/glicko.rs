use crate::config::Config;
use crate::types::{CHMatch, Glicko2HistoryEntry};
use chrono::Duration;
use roots::SimpleConvergency;
use std::collections::HashMap;
use std::f64::consts::{E, PI};

#[tracing::instrument(skip(matches, before_player_ratings))]
pub fn update_player_ratings_all_matches(
    config: &Config,
    account_id: u32,
    matches: &[&CHMatch], // Assume matches are sorted by match_id
    before_player_ratings: &HashMap<u32, Glicko2HistoryEntry>,
) -> Vec<Glicko2HistoryEntry> {
    let mut applied_matches = vec![];
    let mut out = vec![];
    for match_ in matches {
        applied_matches.push(*match_);
        out.push(update_player_rating(
            config,
            account_id,
            &applied_matches,
            before_player_ratings,
        ));
    }
    out
}

pub fn update_player_rating(
    config: &Config,
    account_id: u32,
    matches: &[&CHMatch], // Assume matches are sorted by match_id
    before_player_ratings: &HashMap<u32, Glicko2HistoryEntry>,
) -> Glicko2HistoryEntry {
    // Step 1: Calculate the new rating deviation (`rd`) for the player based on the time since their last match.
    let rating = before_player_ratings
        .get(&account_id)
        .map(|entry| entry.rating)
        .unwrap_or(config.rating_unrated);
    let rating_deviation = match before_player_ratings.get(&account_id) {
        Some(entry) => new_rating_deviation(
            entry.rating_deviation,
            entry.rating_volatility,
            matches[0].start_time - entry.start_time, // matches[0] is safe because we checked that matches is not empty
        ),
        None => config.rating_deviation_unrated, // If the player has no rating history, use the default RD_UNRATED
    };
    let rating_volatility = before_player_ratings
        .get(&account_id)
        .map(|entry| entry.rating_volatility)
        .unwrap_or(config.rating_volatility_unrated);

    let opponents = matches
        .iter()
        .flat_map(|m| {
            let (opponent_team, won) = if m.team0_players.contains(&account_id) {
                (&m.team1_players, m.winning_team == 0)
            } else {
                (&m.team0_players, m.winning_team == 1)
            };
            opponent_team.iter().map(move |opponent_id| {
                let opponent_rating = before_player_ratings
                    .get(opponent_id)
                    .map(|entry| entry.rating)
                    .unwrap_or(config.rating_unrated);
                let opponent_rd = before_player_ratings
                    .get(opponent_id)
                    .map(|entry| entry.rating_deviation)
                    .unwrap_or(config.rating_deviation_unrated);
                (opponent_rating, opponent_rd, won)
            })
        })
        .collect::<Vec<_>>();

    let estimated_variance = 1.
        / opponents
            .iter()
            .map(|(opponent_rating, opponent_rd, _)| {
                let e = e(rating, *opponent_rating, *opponent_rd);
                g(*opponent_rd).powi(2) * e * (1.0 - e)
            })
            .sum::<f64>()
            .max(1e-10);

    let h = opponents
        .iter()
        .map(|(opponent_rating, opponent_rd, won)| {
            g(*opponent_rd) * (*won as u8 as f64 - e(rating, *opponent_rating, *opponent_rd))
        })
        .sum::<f64>();
    let estimated_improvement = estimated_variance * h;

    let mut convergency = SimpleConvergency {
        eps: 1e-10f64,
        max_iter: 100,
    };

    let f = |x: f64| {
        let ratio_1 = x.exp()
            * (estimated_improvement.powi(2)
                - rating_deviation.powi(2)
                - estimated_variance
                - x.exp())
            / (2.0 * (rating_deviation.powi(2) + estimated_variance + x.exp()).powi(2));
        let ratio_2 = (x - rating_volatility.powi(2).ln()) / config.tau;
        ratio_1 - ratio_2
    };

    let a = (rating_volatility * rating_volatility).ln();
    let b = if estimated_improvement * estimated_improvement
        > rating_deviation * rating_deviation + estimated_variance
    {
        (estimated_improvement * estimated_improvement
            - rating_deviation * rating_deviation
            - estimated_variance)
            .ln()
    } else {
        let mut k = 1.0;
        loop {
            let b = a - k * config.tau;
            if f(b) < 0.0 {
                k += 1.0;
            } else {
                break b;
            }
        }
    };

    let root = roots::find_root_regula_falsi(a, b, &f, &mut convergency).unwrap();
    assert!(!root.is_nan());

    let new_rating_volatility = root.exp().sqrt();
    let new_rating_deviation = 1.
        / (1. / (rating_deviation.powi(2) + new_rating_volatility.powi(2))
            + 1. / estimated_variance)
            .sqrt();
    let new_rating = rating + new_rating_deviation.powi(2) * h;
    assert!(!new_rating.is_nan());
    assert!(!new_rating_deviation.is_nan());
    assert!(!new_rating_volatility.is_nan());

    Glicko2HistoryEntry {
        account_id,
        match_id: matches.last().unwrap().match_id, // unwrap is safe because we checked that matches is not empty
        rating: new_rating,
        rating_deviation: new_rating_deviation,
        rating_volatility: new_rating_volatility,
        start_time: matches.last().unwrap().start_time, // unwrap is safe because we checked that matches is not empty
    }
}

fn new_rating_deviation(
    old_rating_deviation: f64,
    old_rating_volatility: f64,
    time_since_last_match: Duration,
) -> f64 {
    (old_rating_deviation.powi(2)
        + old_rating_volatility.powi(2) * time_since_last_match.num_days() as f64)
        .sqrt()
}

fn g(rd: f64) -> f64 {
    1.0 / (1.0 + (3.0 / PI.powi(2)) * rd.powi(2)).sqrt()
}

fn e(rating: f64, rating_opponent: f64, rd_opponent: f64) -> f64 {
    1.0 / (1.0 + E.powf(-g(rd_opponent) * (rating - rating_opponent)))
}
