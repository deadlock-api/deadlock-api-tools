use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use metrics::counter;
use metrics_exporter_prometheus::{BuildError, PrometheusBuilder};
use once_cell::sync::Lazy;
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::net::SocketAddrV4;
use std::time::Duration;
use tracing::instrument;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use valveprotos::deadlock::EgcCitadelClientMessages;

static STEAM_PROXY_URL: Lazy<String> = Lazy::new(|| std::env::var("STEAM_PROXY_URL").unwrap());
static STEAM_PROXY_API_KEY: Lazy<String> =
    Lazy::new(|| std::env::var("STEAM_PROXY_API_KEY").unwrap());

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SteamProxyResponse {
    pub data: String,
    pub username: String,
}

#[allow(clippy::too_many_arguments)]
#[instrument(skip(http_client, msg))]
pub async fn call_steam_proxy<T: Message + Default>(
    http_client: &reqwest::Client,
    msg_type: EgcCitadelClientMessages,
    msg: impl Message,
    in_all_groups: Option<&[&str]>,
    in_any_groups: Option<&[&str]>,
    cooldown_time: Duration,
    request_timeout: Duration,
) -> reqwest::Result<(String, T)> {
    let serialized_message = msg.encode_to_vec();
    let encoded_message = BASE64_STANDARD.encode(&serialized_message);
    let result = http_client
        .post(&*STEAM_PROXY_URL)
        .bearer_auth(&*STEAM_PROXY_API_KEY)
        .timeout(request_timeout)
        .json(&json!({
            "message_kind": msg_type as i32,
            "job_cooldown_millis": cooldown_time.as_millis(),
            "rate_limit_cooldown_millis": 2 * cooldown_time.as_millis(),
            "bot_in_all_groups": in_all_groups,
            "bot_in_any_groups": in_any_groups,
            "data": encoded_message,
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .map(|r: SteamProxyResponse| (r.username, BASE64_STANDARD.decode(&r.data).unwrap()))
        .map(|(username, data)| (username, T::decode(data.as_ref()).unwrap()));
    match result {
        Ok(_) => {
            counter!("steam_proxy.call.success", "msg_type" => msg_type.as_str_name().to_string())
                .increment(1)
        }
        Err(_) => {
            counter!("steam_proxy.call.failure", "msg_type" => msg_type.as_str_name().to_string())
                .increment(1)
        }
    }
    result
}

pub fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new(
        "debug,h2=warn,hyper_util=warn,reqwest=warn,rustls=warn,sqlx=warn,steam_vent=info",
    ));
    let fmt_layer = tracing_subscriber::fmt::layer();

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(env_filter)
        .init();
}

pub fn init_metrics() -> Result<(), BuildError> {
    PrometheusBuilder::new()
        .with_http_listener("0.0.0.0:9002".parse::<SocketAddrV4>().unwrap())
        .install()
}
