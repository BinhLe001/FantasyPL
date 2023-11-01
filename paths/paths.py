from datetime import date
from paths.path_builder import PathBuilder


def teams_path(run_date: date) -> PathBuilder:
    return PathBuilder("team", run_date)


def standings_path(run_date: date) -> PathBuilder:
    return PathBuilder("standings", run_date)


def matches_path(run_date: date) -> PathBuilder:
    return PathBuilder("matches", run_date)


def players_path(run_date: date) -> PathBuilder:
    return PathBuilder("players", run_date)


def draft_path(run_date: date) -> PathBuilder:
    return PathBuilder("draft", run_date)


def positions_path(run_date: date) -> PathBuilder:
    return PathBuilder("positions", run_date)
