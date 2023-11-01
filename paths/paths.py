from datetime import date
from path_builder import PathBuilder


def teams_path(run_date: date) -> PathBuilder:
    return PathBuilder("team", run_date)


def players_path(run_date: date) -> PathBuilder:
    return PathBuilder("players", run_date)


def draft_path(run_date: date) -> PathBuilder:
    return PathBuilder("draft", run_date)


def positions_path(run_date: date) -> PathBuilder:
    return PathBuilder("positions", run_date)
