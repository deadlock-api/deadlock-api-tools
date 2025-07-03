import logging
import os
from typing import List

import optuna
from clickhouse_driver import Client
from optuna import Trial
from pydantic import BaseModel, TypeAdapter
from tqdm import tqdm


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)
UPDATE_SENSITIVITY = 32
LEARNING_RATE = 0.9

ch_client = Client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_PORT", 9000)),
    user=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    database=os.getenv("CLICKHOUSE_DB", "default"),
)

RANKS = [
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
    116,
]


class MatchTeam(BaseModel):
    players: list[int]
    average_badge_team: int
    won: bool


class Match(BaseModel):
    match_id: int
    teams: list[MatchTeam]


def get_matches_starting_from(client, start_id: int = 28626948) -> list[Match]:
    query = f"""
    SELECT match_id,
           groupArrayIf(account_id, team = 'Team0') as team0_players,
           groupArrayIf(account_id, team = 'Team1') as team1_players,
           any(average_badge_team0)                 as avg_badge_team0,
           any(average_badge_team1)                 as avg_badge_team1,
           any(winning_team)                        as winning_team
    FROM match_player FINAL
        INNER JOIN match_info mi FINAL USING (match_id)
    WHERE match_mode IN ('Ranked', 'Unranked')
      AND average_badge_team0 IS NOT NULL
      AND average_badge_team1 IS NOT NULL
      AND match_id > {start_id}
      AND low_pri_pool != true
    GROUP BY match_id
    HAVING length(team0_players) = 6 AND length(team1_players) = 6
    ORDER BY match_id
    """
    result = client.execute(query)
    return [
        Match(
            match_id=row[0],
            teams=[
                MatchTeam(
                    players=row[1], average_badge_team=row[3], won=row[5] == "Team0"
                ),
                MatchTeam(
                    players=row[2], average_badge_team=row[4], won=row[5] == "Team1"
                ),
            ],
        )
        for row in result
    ]


def get_regression_starting_id(client) -> int:
    min_created_at_query = """
    SELECT start_time
    FROM mmr_history2 FINAL
    INNER JOIN match_info FINAL USING (match_id)
    WHERE match_outcome = 'TeamWin'
      AND match_mode IN ('Ranked', 'Unranked')
      AND game_mode = 'Normal'
      AND average_badge_team0 IS NOT NULL
      AND average_badge_team1 IS NOT NULL
      AND low_pri_pool != true
    ORDER BY match_id DESC
    LIMIT 1
    """
    results = client.execute(min_created_at_query)
    if len(results) == 0:
        min_created_at = "2024-01-01 00:00:00"
    else:
        min_created_at = client.execute(min_created_at_query)[0][0].isoformat()

    query = f"""
    SELECT match_id
    FROM match_info FINAL
    WHERE match_outcome = 'TeamWin'
        AND match_mode IN ('Ranked', 'Unranked')
        AND game_mode = 'Normal'
        AND average_badge_team0 IS NOT NULL
        AND average_badge_team1 IS NOT NULL
        AND created_at > '{min_created_at}'
        AND match_id > 28626948
      AND low_pri_pool != true
    ORDER BY created_at
    LIMIT 1
    """
    return client.execute(query)[0][0]


def get_all_player_mmrs(client, at_match_id: int) -> dict[int, float]:
    query = f"""
    SELECT account_id, player_score
    FROM mmr_history2 FINAL
    WHERE match_id <= {at_match_id}
    ORDER BY account_id, match_id DESC
    LIMIT 1 BY account_id;
    """
    result = client.execute(query)
    return {row[0]: row[1] for row in result}


def set_player_mmr(client, data: list[tuple[int, dict[int, float]]]):
    client.execute(
        """
        INSERT INTO mmr_history2 (account_id, match_id, player_score)
        VALUES
        """,
        [
            {"account_id": account_id, "match_id": match_id, "player_score": mmr}
            for match_id, player_mmr in data
            for account_id, mmr in player_mmr.items()
        ],
    )


def expected_outcome(average_team0: float, average_team1: float) -> float:
    return 1 / (1 + 10 ** ((average_team1 - average_team0) / 400))


def run_regression(
    match: Match,
    all_player_mmrs: dict[int, float],
    update_sensitivity: float = UPDATE_SENSITIVITY,
    learning_rate: float = LEARNING_RATE,
) -> (dict[int, float], float):
    assert len(match.teams) == 2, "Match must have exactly two teams"

    if (
        match.teams[0].average_badge_team == 116
        and match.teams[1].average_badge_team == 116
    ):
        return {}, 0

    avg_team0_rank_true = RANKS.index(match.teams[0].average_badge_team)
    avg_team1_rank_true = RANKS.index(match.teams[1].average_badge_team)

    team0_ranks = {
        p_id: all_player_mmrs.get(p_id, avg_team0_rank_true)
        for p_id in match.teams[0].players
    }
    team1_ranks = {
        p_id: all_player_mmrs.get(p_id, avg_team1_rank_true)
        for p_id in match.teams[1].players
    }

    avg_team0_rank_pred = sum(team0_ranks.values()) / len(team0_ranks)
    avg_team1_rank_pred = sum(team1_ranks.values()) / len(team1_ranks)
    error0 = (avg_team0_rank_true - avg_team0_rank_pred) / len(team0_ranks)
    error1 = (avg_team1_rank_true - avg_team1_rank_pred) / len(team1_ranks)

    expected0 = expected_outcome(avg_team0_rank_pred, avg_team1_rank_pred)
    outcome0 = 1 if match.teams[0].won else 0
    team0_ranks = {
        p_id: p_mmr + update_sensitivity * (outcome0 - expected0)
        for p_id, p_mmr in team0_ranks.items()
    }
    team1_ranks = {
        p_id: p_mmr + update_sensitivity * (expected0 - outcome0)
        for p_id, p_mmr in team1_ranks.items()
    }

    avg_team0_rank_pred = sum(team0_ranks.values()) / len(team0_ranks)
    avg_team1_rank_pred = sum(team1_ranks.values()) / len(team1_ranks)
    new_error0 = (avg_team0_rank_true - avg_team0_rank_pred) / len(team0_ranks)
    new_error1 = (avg_team1_rank_true - avg_team1_rank_pred) / len(team1_ranks)

    updates = {
        **{i: r + learning_rate * new_error0 for i, r in team0_ranks.items()},
        **{i: r + learning_rate * new_error1 for i, r in team1_ranks.items()},
    }
    error = (abs(error0) + abs(error1)) / 2
    return updates, error


ta = TypeAdapter(List[Match])
if os.path.exists("data.json"):
    with open("data.json", "rb") as f:
        matches = ta.validate_json(f.read())
else:
    with ch_client as client:
        starting_id = get_regression_starting_id(client)
        matches = get_matches_starting_from(client, starting_id)
        with open("data.json", "wb") as f:
            f.write(ta.dump_json(matches))


def objective(trial: Trial) -> float:
    update_sensitivity = trial.suggest_float("update_sensitivity", 0, 2)
    learning_rate = trial.suggest_float("learning_rate", 0.1, 1.5)
    LOGGER.info(f"Running trial with {update_sensitivity=}, {learning_rate=}")
    all_player_mmrs = {}
    sum_error = 0.0
    for i, match in tqdm(enumerate(matches), desc="Processing matches"):
        updated_mmrs, error = run_regression(
            match, all_player_mmrs, update_sensitivity, learning_rate
        )
        sum_error += error
        all_player_mmrs.update(updated_mmrs)
    return sum_error / max(1, len(matches))


if __name__ == "__main__":
    study = optuna.create_study()
    study.optimize(objective, n_trials=100)
    LOGGER.info(
        f"Best params: {study.best_trial.params}; Best trial: {study.best_trial.value}"
    )
