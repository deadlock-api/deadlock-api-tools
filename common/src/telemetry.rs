use metrics_exporter_prometheus::{BuildError, PrometheusBuilder};
use std::net::SocketAddrV4;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new(
        "debug,h2=warn,hyper_util=warn,reqwest=warn,rustls=warn,sqlx=warn,steam_vent=info,opentelemetry_sdk=info,tower=info,opentelemetry-otlp=info",
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
