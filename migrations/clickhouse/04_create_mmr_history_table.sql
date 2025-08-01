DROP TABLE IF EXISTS mmr_history;

CREATE TABLE IF NOT EXISTS mmr_history
(
 account_id    UInt32,
 match_id      UInt64,
 player_score  Float64,
 rank          UInt32 ALIAS [
  0,
  11,
  12,
  13,
  14,
  15,
  16,
  21,
  22,
  23,
  24,
  25,
  26,
  31,
  32,
  33,
  34,
  35,
  36,
  41,
  42,
  43,
  44,
  45,
  46,
  51,
  52,
  53,
  54,
  55,
  56,
  61,
  62,
  63,
  64,
  65,
  66,
  71,
  72,
  73,
  74,
  75,
  76,
  81,
  82,
  83,
  84,
  85,
  86,
  91,
  92,
  93,
  94,
  95,
  96,
  101,
  102,
  103,
  104,
  105,
  106,
  111,
  112,
  113,
  114,
  115,
  116
  ][toUInt8(clamp(player_score, 0, 66) + 1)],
 division      UInt32 ALIAS floor(rank / 10),
 division_tier UInt32 ALIAS rank % 10,
  start_time DATETIME
) ENGINE = ReplacingMergeTree
   ORDER BY (account_id, match_id)
   SETTINGS allow_nullable_key = 1;

DROP TABLE IF EXISTS hero_mmr_history;

CREATE TABLE IF NOT EXISTS hero_mmr_history
(
 account_id    UInt32,
 match_id      UInt64,
 hero_id       UInt32,
 player_score  Float64,
 rank          UInt32 ALIAS [
  0,
  11,
  12,
  13,
  14,
  15,
  16,
  21,
  22,
  23,
  24,
  25,
  26,
  31,
  32,
  33,
  34,
  35,
  36,
  41,
  42,
  43,
  44,
  45,
  46,
  51,
  52,
  53,
  54,
  55,
  56,
  61,
  62,
  63,
  64,
  65,
  66,
  71,
  72,
  73,
  74,
  75,
  76,
  81,
  82,
  83,
  84,
  85,
  86,
  91,
  92,
  93,
  94,
  95,
  96,
  101,
  102,
  103,
  104,
  105,
  106,
  111,
  112,
  113,
  114,
  115,
  116
  ][toUInt8(clamp(player_score, 0, 66) + 1)],
 division      UInt32 ALIAS floor(rank / 10),
 division_tier UInt32 ALIAS rank % 10,
 start_time DATETIME
) ENGINE = ReplacingMergeTree
   ORDER BY (hero_id, account_id, match_id)
   SETTINGS allow_nullable_key = 1;

DROP TABLE IF EXISTS glicko;

CREATE TABLE IF NOT EXISTS glicko
(
 account_id       UInt32,
 match_id         UInt64,
 rating_mu        Float64,
 rating_phi       Float64,
 rating_sigma     Float64,
 rating           Float64 ALIAS rating_mu * 173.7178 + 1500,
 rating_deviation Float64 ALIAS rating_phi * 173.7178,
 start_time       DateTime
) ENGINE = ReplacingMergeTree
   ORDER BY (account_id, match_id);

ALTER TABLE glicko
 ADD COLUMN player_score Float64 ALIAS (rating_mu + 6) * 11. / 2.,
 ADD COLUMN  rank          UInt32 ALIAS [
  0,
  11,
  12,
  13,
  14,
  15,
  16,
  21,
  22,
  23,
  24,
  25,
  26,
  31,
  32,
  33,
  34,
  35,
  36,
  41,
  42,
  43,
  44,
  45,
  46,
  51,
  52,
  53,
  54,
  55,
  56,
  61,
  62,
  63,
  64,
  65,
  66,
  71,
  72,
  73,
  74,
  75,
  76,
  81,
  82,
  83,
  84,
  85,
  86,
  91,
  92,
  93,
  94,
  95,
  96,
  101,
  102,
  103,
  104,
  105,
  106,
  111,
  112,
  113,
  114,
  115,
  116
  ][toUInt8(clamp(player_score, 0, 66) + 1)],
 ADD COLUMN division      UInt32 ALIAS floor(rank / 10),
 ADD COLUMN division_tier UInt32 ALIAS rank % 10;
