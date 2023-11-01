from pyspark.sql import SparkSession
from typing import Dict
from external.api import (
    fetch_league_details,
    fetch_draft_details,
    fetch_static_details,
)
from paths import paths
from datetime import date
from helpers.defs import DATE_FORMAT
from helpers.spark import get_spark_session


def create_non_null_dataset(json_data: Dict) -> Dict:
    non_null_keys = set()
    for event in json_data:
        for key, value in event.items():
            if value:
                non_null_keys.add(key)
    non_null_data = []
    for event in json_data:
        new_event = {}
        for key, value in event.items():
            if key in non_null_keys:
                new_event[key] = value
        non_null_data.append(new_event)
    return non_null_data


def store_teams_df(spark: SparkSession, teams_json: Dict, run_date: date) -> None:
    """
    Initial schema:
    +--------+-------------------+------+--------------------+-----------------+----------------+----------+-----------+
    |entry_id|         entry_name|    id|         joined_time|player_first_name|player_last_name|short_name|waiver_pick|
    +--------+-------------------+------+--------------------+-----------------+----------------+----------+-----------+

    Final schema:
    +-------------------+------+----------+---------+----------+-----------+
    |          team_name|    id|first_name|last_name|short_name|waiver_pick|
    +-------------------+------+----------+---------+----------+-----------+
    """
    teams_df = spark.createDataFrame(create_non_null_dataset(teams_json))
    teams_df = (
        teams_df.drop("joined_time")
        .drop("entry_id")
        .withColumnRenamed("entry_name", "team_name")
        .withColumnRenamed("player_first_name", "first_name")
        .withColumnRenamed("player_last_name", "last_name")
    )
    path = paths.teams_path(run_date).next_path()
    teams_df.repartition(1).write.csv(path, header=True)


def store_standings_df(
    spark: SparkSession, standings_json: Dict, run_date: date
) -> None:
    """
    Initial schema:
    +---------+------------+------------+--------------+-----------+--------------+----------+----+---------+-----+
    |last_rank|league_entry|matches_lost|matches_played|matches_won|points_against|points_for|rank|rank_sort|total|
    +---------+------------+------------+--------------+-----------+--------------+----------+----+---------+-----+

    Final schema:
    +---------+-------+------------+--------------+-----------+--------------+----------+----+---------+-----+
    |last_rank|team_id|matches_lost|matches_played|matches_won|points_against|points_for|rank|rank_sort|total|
    +---------+-------+------------+--------------+-----------+--------------+----------+----+---------+-----+
    """
    standings_df = spark.createDataFrame(create_non_null_dataset(standings_json))
    standings_df = standings_df.withColumnRenamed("league_entry", "team_id")
    path = paths.standings_path(run_date).next_path()
    standings_df.repartition(1).write.csv(path, header=True)


def store_matches_df(spark: SparkSession, matches_json: Dict, run_date: date) -> None:
    """
    Initial schema:
    +-----+--------+--------------+---------------------+--------------+---------------------+-------+
    |event|finished|league_entry_1|league_entry_1_points|league_entry_2|league_entry_2_points|started|
    +-----+--------+--------------+---------------------+--------------+---------------------+-------+

    Final schema:
    +----+--------+---------+-------------+---------+-------------+-------+
    |week|finished|team_1_id|team_1_points|team_2_id|team_2_points|started|
    +----+--------+---------+-------------+---------+-------------+-------+
    """
    matches_df = spark.createDataFrame(create_non_null_dataset(matches_json))
    matches_df = (
        matches_df.withColumnRenamed("event", "week")
        .withColumnRenamed("league_entry_1", "team_1_id")
        .withColumnRenamed("league_entry_2", "team_2_id")
        .withColumnRenamed("league_entry_1_points", "team_1_points")
        .withColumnRenamed("league_entry_2_points", "team_2_points")
    )
    path = paths.matches_path(run_date).next_path()
    matches_df.repartition(1).write.csv(path, header=True)


def store_draft_df(spark: SparkSession, draft_json: Dict, run_date: date) -> None:
    """
    Initial schema:
    +--------------------+-------+------+-------------------+-------+-----+------+----+-----------------+----------------+-----+--------+
    |         choice_time|element| entry|         entry_name|     id|index|league|pick|player_first_name|player_last_name|round|was_auto|
    +--------------------+-------+------+-------------------+-------+-----+------+----+-----------------+----------------+-----+--------+

    Final schema:
    +--------------------+---------+-------+-------------------+-------+-----+------+----+-----+--------+
    |         choice_time|player_id|team_id|          team_name|     id|index|league|pick|round|was_auto|
    +--------------------+---------+-------+-------------------+-------+-----+------+----+-----+--------+
    """
    draft_df = spark.createDataFrame(create_non_null_dataset(draft_json))
    draft_df = (
        draft_df.withColumnRenamed("element", "player_id")
        .withColumnRenamed("entry", "team_id")
        .withColumnRenamed("entry_name", "team_name")
        .drop("player_first_name", "player_last_name")
    )
    path = paths.draft_path(run_date).next_path()
    draft_df.repartition(1).write.csv(path, header=True)


def store_players_df(spark: SparkSession, players_json: Dict, run_date: date) -> None:
    """
    Schemas too large to display
    """
    players_df = spark.createDataFrame(create_non_null_dataset(players_json))
    players_df = (
        players_df.withColumnRenamed("element_type", "position_id")
        .withColumnRenamed("element_id", "id")
        .withColumnRenamed("team", "club_id")
    )
    path = paths.players_path(run_date).next_path()
    players_df.repartition(1).write.csv(path, header=True)


def store_positions_df(
    spark: SparkSession, positions_json: Dict, run_date: date
) -> None:
    """
    Initial_schema:
    +-------------+---+-----------+-----------------+-------------+-------------------+
    |element_count| id|plural_name|plural_name_short|singular_name|singular_name_short|
    +-------------+---+-----------+-----------------+-------------+-------------------+

    Final schema:
    +-------------+---+-----------+-----------------+-------------+-------------------+
    |        count| id|plural_name|plural_name_short|singular_name|singular_name_short|
    +-------------+---+-----------+-----------------+-------------+-------------------+
    """
    positions_df = spark.createDataFrame(create_non_null_dataset(positions_json))
    positions_df = (
        positions_df.withColumnRenamed("element_count", "count")
        .withColumnRenamed("singular_name", "position_name")
        .withColumnRenamed("singular_name_short", "position_name_short")
        .drop("plural_name", "plural_name_short")
    )
    path = paths.positions_path(run_date).next_path()
    positions_df.repartition(1).write.csv(path, header=True)


if __name__ == "__main__":
    run_date = date.today()
    print(f"RUNNING DATAFRAM STORE FOR {run_date.strftime(DATE_FORMAT)}")
    print("Creating spark session...")
    spark = get_spark_session()

    print("Fetching league details...")
    league_details = fetch_league_details()
    print("STORING TEAMS DF")
    store_teams_df(spark, league_details["league_entries"], run_date)
    print("STORING STANDINGS DF")
    store_standings_df(spark, league_details["standings"], run_date)
    print("STORING MATCHES DF")
    store_matches_df(spark, league_details["matches"], run_date)

    print("Fetching draft details...")
    draft_details = fetch_draft_details()
    print("STORING DRAFT DF")
    store_draft_df(spark, draft_details["choices"], run_date)

    print("Fetching static details...")
    static_details = fetch_static_details()
    print("STORING PLAYERS DF")
    store_players_df(spark, static_details["elements"], run_date)
    print("STORING POSITIONS DF")
    store_positions_df(spark, static_details["element_types"], run_date)
    print("DONE!")
