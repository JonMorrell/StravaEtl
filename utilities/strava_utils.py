import pandas as pd
import requests
import configparser
import urllib3
import re

from typing import Dict
from datetime import datetime

def connect_strava() -> Dict[str, str]:
    """Get the Strava API connection info and return header."""
    # get strava api info
    parser = configparser.ConfigParser()
    parser.read("config.ini")

    auth_url = parser.get("STRAVA_API", "AUTH_ENDPOINT")
    client_id = parser.get("STRAVA_API", "CLIENT_ID")
    client_secret = parser.get("STRAVA_API", "CLIENT_SECRET")
    refresh_token = parser.get("STRAVA_API", "REFRESH_TOKEN")

    # connect to API
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
        "f": "json",
    }

    # get access token from result and return it in a header dictionary
    result = requests.post(auth_url, data=payload, verify=False)
    access_token = result.json()["access_token"]
    header = {"Authorization": "Bearer " + access_token}
    return header

def make_strava_api_request(header:Dict, activity_num:int)-> Dict[str, str]:
    """Use Strava API to get recent page of new data."""
    parser = configparser.ConfigParser()
    parser.read("config.ini")
    get_activities_url = parser.get("STRAVA_API", "ACTIVITIES_ENDPOINT")
    param = {"per_page": 1, "page": activity_num}
    
    return requests.get(get_activities_url, headers=header, params=param).json()[0]

def convert_strava_start_date(date: str) -> datetime:
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    converted_date = datetime.strptime(date, date_format)
    return converted_date

def parse_api_output(response_json: dict) -> dict:
    """Parse output from Strava API into a new cut-down and cleansed json dictionary."""
    new_dic = {}
    normalised_response = pd.json_normalize(response_json)

    cols_to_extract = [
        "id",
        "name",
        "distance",
        "moving_time",
        "elapsed_time",
        "total_elevation_gain",
        "type",
        "workout_type",
        "location_country",
        "achievement_count",
        "kudos_count",
        "comment_count",
        "athlete_count",
        "photo_count",
        "average_speed",
        "max_speed",
        "average_cadence",
        "average_temp",
        "average_heartrate",
        "max_heartrate",
        "suffer_score",
    ]
    for col in cols_to_extract:
        try:
            new_dic[col] = response_json[col]
        except KeyError:
            new_dic[col] = None

    try:
        new_dic["start_date"] = convert_strava_start_date(response_json["start_date"])
    except KeyError:
        new_dic["start_date"] = None

    try:
        # remove timezone info
        timezone = response_json["timezone"]
        timezone = re.sub("[\(\[].*?[\)\]]", "", timezone) # type: ignore
        new_dic["timezone"] = timezone[1:]
    except KeyError:
        new_dic["timezone"] = None

    # for the pretty tableau map!
    try:
        start_latlng = response_json["start_latlng"]
        if len(start_latlng) == 2:
            lat, lng = start_latlng[0], start_latlng[1]
            new_dic["lat"] = lat
            new_dic["lng"] = lng
        else:
            new_dic["lat"] = None
            new_dic["lng"] = None
    except KeyError:
        new_dic["lat"] = None
        new_dic["lng"] = None
        
    return new_dic