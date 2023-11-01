import json
import requests
from typing import Dict
from helpers.defs import LEAGUE_ID


def fetch_league_details() -> Dict:
    fpl_league_details = json.loads(
        requests.get(
            f"https://draft.premierleague.com/api/league/{LEAGUE_ID}/details"
        ).text
    )
    return fpl_league_details


def fetch_draft_details() -> Dict:
    fpl_draft_details = json.loads(
        requests.get(
            f"https://draft.premierleague.com/api/draft/{LEAGUE_ID}/choices"
        ).text
    )
    return fpl_draft_details


def fetch_static_details() -> Dict:
    fpl_static_details = json.loads(
        requests.get(f"https://draft.premierleague.com/api/bootstrap-static").text
    )
    return fpl_static_details
