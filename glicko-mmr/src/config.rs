use clap::Parser;

#[derive(Parser, Debug, Copy, Clone, PartialEq)]
#[command(version, about, long_about = None)]
pub struct Config {
    /// Default rating for unrated players.
    /// Set via --rating-unrated or RATING_UNRATED.
    #[arg(long, env, default_value_t = 0.0)]
    pub rating_unrated: f64,

    /// Default rating deviation for unrated players.
    /// Set via --rating-deviation-unrated or RATING_DEVIATION_UNRATED.
    #[arg(long, env, default_value_t = 2.576)] // Hyper parameter tuned
    pub rating_deviation_unrated: f64,

    /// Default rating volatility for unrated players.
    /// Set via --rating-volatility-unrated or RATING_VOLATILITY_UNRATED.
    #[arg(long, env, default_value_t = 0.06)]
    pub rating_volatility_unrated: f64,

    #[arg(long, env, default_value_t = 0.5)]
    pub tau: f64,
}
