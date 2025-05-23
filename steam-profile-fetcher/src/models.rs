use serde::{Deserialize, Serialize};

// Steam API response structures
#[derive(Debug, Serialize, Deserialize)]
pub struct SteamPlayerSummaryResponse {
    pub response: SteamPlayerSummaryResponseInner,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SteamPlayerSummaryResponseInner {
    pub players: Vec<SteamPlayerSummary>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SteamPlayerSummary {
    pub steamid: String,
    pub personaname: String,
    pub profileurl: String,
    pub avatar: String,
    pub personastate: u8,
    pub realname: Option<String>,
    pub loccountrycode: Option<String>,
}
