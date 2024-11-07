DROP VIEW IF EXISTS player;
CREATE MATERIALIZED VIEW player
REFRESH EVERY 10 MINUTES
ENGINE = ReplacingMergeTree() ORDER BY account_id
POPULATE AS
 SELECT players.account_id as account_id, region_mode
 FROM active_matches
 ARRAY JOIN players
 GROUP BY `players.account_id`, region_mode
 ORDER BY COUNT(*) DESC
 LIMIT 1 by account_id
UNION DISTINCT
 WITH active_player_region AS (
   SELECT players.account_id as account_id, region_mode
   FROM active_matches
   ARRAY JOIN players
   GROUP BY `players.account_id`, region_mode
   ORDER BY COUNT(*) DESC
   LIMIT 1 by account_id
 )
 SELECT mp.account_id, apr.region_mode
 FROM match_player mp
       LEFT JOIN match_player mates ON mp.match_id = mates.match_id
       LEFT JOIN active_player_region apr ON apr.account_id = mates.account_id
 WHERE mp.account_id NOT IN (SELECT account_id FROM active_player_region)
   AND mp.account_id != mates.account_id
SETTINGS allow_experimental_refreshable_materialized_view = 1;
