from helpers.spark import get_spark_session
from datetime import date
from paths.paths import draft_path, players_path, positions_path
import pyspark.sql.functions as psf
from pyspark.sql import DataFrame, SparkSession
from typing import Optional


def get_drafted_players_in_team(
    team_name: str, run_date: date, spark: Optional[SparkSession] = None
) -> DataFrame:
    spark = spark if spark else get_spark_session()
    draft_df = spark.read.csv(draft_path(run_date).latest_path(), header=True).filter(
        psf.col("team_name") == team_name
    )
    if draft_df.count() == 0:
        raise ValueError(f"Team name {team_name} is not a valid team.")

    players_df = spark.read.csv(
        players_path(run_date).latest_path(), header=True
    ).withColumnRenamed("id", "player_id")

    positions_df = spark.read.csv(
        positions_path(run_date).latest_path(), header=True
    ).withColumnRenamed("id", "position_id")

    return (
        draft_df.join(players_df, on="player_id", how="inner")
        .join(positions_df, on="position_id", how="inner")
        .select(
            "team_name",
            "web_name",
            "goals_scored",
            "assists",
            "expected_goals",
            "expected_goal_involvements",
            psf.col("singular_name").alias("position"),
            "total_points",
            "red_cards",
            "yellow_cards",
        )
    )
