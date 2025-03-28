use clickhouse::Row;
use serde::Deserialize;
use std::fmt::Debug;

#[derive(Row, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct MatchSalts {
    pub match_id: u64,
    pub cluster_id: Option<u32>,
    pub metadata_salt: Option<u32>,
    pub replay_salt: Option<u32>,
}

impl Debug for MatchSalts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatchSalts")
            .field("match_id", &self.match_id)
            .finish()
    }
}
