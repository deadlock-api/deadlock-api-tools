use cached::proc_macro::cached;
use serde::{Deserialize, Serialize};
use tracing::info;
use valveprotos::deadlock::ECitadelTeamObjective;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScrapePriority {
    rank: u32,
    objectives: u32,
    duration_s: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ActiveMatch {
    pub start_time: Option<u64>,
    pub match_id: u64,
    pub lobby_id: Option<u64>,
    pub spectators: Option<u32>,
    pub objectives_mask_team0: u32,
    pub objectives_mask_team1: u32,
    pub match_score: Option<u32>,
}

#[allow(unused)]
impl ActiveMatch {
    pub fn is_core_exposed(&self) -> bool {
        use ECitadelTeamObjective::*;
        let t0 = self.objectives_mask_team0;
        let t1 = self.objectives_mask_team1;

        !has_objective(t0, KECitadelTeamObjectiveTitan)
            || !has_objective(t1, KECitadelTeamObjectiveTitan)
    }
    pub fn is_titan_exposed(&self) -> bool {
        use ECitadelTeamObjective::*;
        let t0 = self.objectives_mask_team0;
        let t1 = self.objectives_mask_team1;

        (!has_objective(t0, KECitadelTeamObjectiveTitanShieldGenerator1)
            && !has_objective(t0, KECitadelTeamObjectiveTitanShieldGenerator2))
            || (!has_objective(t1, KECitadelTeamObjectiveTitanShieldGenerator1)
                && !has_objective(t1, KECitadelTeamObjectiveTitanShieldGenerator2))
    }
    pub fn is_shrine_exposed(&self) -> bool {
        use ECitadelTeamObjective::*;
        let t0 = self.objectives_mask_team0;
        let t1 = self.objectives_mask_team1;

        !has_objective(t0, KECitadelTeamObjectiveTitanShieldGenerator1)
            || !has_objective(t0, KECitadelTeamObjectiveTitanShieldGenerator2)
            || !has_objective(t1, KECitadelTeamObjectiveTitanShieldGenerator1)
            || !has_objective(t1, KECitadelTeamObjectiveTitanShieldGenerator2)
    }
}

fn has_objective(mask: u32, objective: ECitadelTeamObjective) -> bool {
    mask & (1 << (objective as u32)) != 0
}

#[cached(result = true, time = 15, result_fallback = true)]
pub async fn fetch_active_matches_cached() -> anyhow::Result<Vec<ActiveMatch>> {
    let client = reqwest::Client::new();
    let res = client
        .get("https://api.deadlock-api.com/v1/matches/active")
        .send()
        .await?;

    let active_matches: Vec<ActiveMatch> = res.json().await?;
    info!("Fetched new active matches, size: {}", active_matches.len());

    Ok(active_matches)
}
