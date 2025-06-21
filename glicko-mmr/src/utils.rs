use tracing::debug;

pub async fn get_rating_period_starting(
    ch_client: &clickhouse::Client,
) -> clickhouse::error::Result<u32> {
    debug!("Fetching rating period starting id");
    ch_client
        .query(
            r#"
    WITH t_matches as (SELECT match_id FROM glicko FINAL)
    SELECT toStartOfDay(start_time) as day
    FROM match_info FINAL
    WHERE match_mode IN ('Ranked', 'Unranked')
        AND start_time >= '2025-01-01'
        AND match_id NOT IN t_matches
        AND low_pri_pool != true
    GROUP BY day
    HAVING COUNT(DISTINCT match_id) >= 100
    ORDER BY day
    LIMIT 1
    "#,
        )
        .fetch_one()
        .await
}
