from datetime import date
import os


class PathBuilder:
    def __init__(
        self,
        name: str,
        run_date: date,
        extension: str = "csv",
    ):
        self.name = name
        self.run_date = run_date
        self.extension = extension
        self.current_version = self.latest_version()
        self.next_version = self.next_version()

    def build_date(self):
        return f"year={self.run_date.year}/month={self.run_date.month}/day={self.run_date.day}"

    def build_key(self):
        return f"data/{self.name}/{self.build_date()}"

    def latest_path(self):
        return f"{self.build_key()}/version={self.current_version}/{self.name}.{self.extension}"

    def next_path(self):
        return f"{self.build_key()}/version={self.next_version}/{self.name}.{self.extension}"

    def latest_version(self):
        try:
            files = os.listdir(self.build_key())
        except FileNotFoundError:
            return 0
        if not files:
            return 0
        latest_version = int(files[-1].strip("version="))
        return latest_version

    def next_version(self):
        return self.current_version + 1


def teams_path(run_date: date) -> PathBuilder:
    return PathBuilder("team", run_date)


def players_path(run_date: date) -> PathBuilder:
    return PathBuilder("players", run_date)


def draft_path(run_date: date) -> PathBuilder:
    return PathBuilder("draft", run_date)


def positions_path(run_date: date) -> PathBuilder:
    return PathBuilder("positions", run_date)
