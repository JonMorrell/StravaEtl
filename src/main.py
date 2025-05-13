import time
import numpy as np
import pandas as pd

from datetime import datetime
from typing import Dict, List, Tuple
from utilities.database_utils import add_to_db, connect_sql_server, get_columns_dictionary
from utilities.strava_utils import connect_strava, convert_strava_start_date, make_strava_api_request, parse_api_output
from utilities.general_utils import clean_data

def main():
    last_update_datetime, current_datetime = get_last_update_time()
    new_activities = extract_new_strava_activities(last_update_datetime)
    if len(new_activities) > 0:
        add_new_activities_to_db(new_activities)
        add_last_updated_time_to_db(current_datetime)

def get_last_update_time() -> Tuple[datetime, datetime]:
    """Get the datetime we last extracted data (Stored in local db). Also return current datetime."""
    conn = connect_sql_server()
    last_updated_query = "SELECT MAX(updated_datetime) FROM update_history"
    cursor = conn.cursor()
    cursor.execute(last_updated_query)
    # this funky syntax converts the returned row into its column type (datetime)
    last_updated_db = [row[0] for row in cursor.fetchall()][0]
    current_datetime = datetime.today().now()
    return last_updated_db, current_datetime  # type: ignore

def add_last_updated_time_to_db(current_datetime:datetime):
    """Store current datetime in the db as a record of the last extraction datetime."""
    formatted_date = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    conn = connect_sql_server()
    insert_statement = f"INSERT INTO update_history ([updated_datetime]) VALUES ('{formatted_date}')"
    cursor = conn.cursor()
    cursor.execute(insert_statement)
    conn.commit()
    cursor.close()
    conn.close()

def extract_new_strava_activities(last_updated_warehouse: datetime) -> List[List]:
    """Connect to Strava API and get new data since last_updated_warehouse datetime."""
    header = connect_strava()
    all_activities = []
    activity_num = 1

    while True: # while activity not yet extracted

        if activity_num % 75 == 0:
            print("Hit request limit, sleep for 15 minutes...")
            time.sleep(15 * 60)

        response_json = make_strava_api_request(header, activity_num)

        if len(response_json) == 0:
            break

        converted_date = convert_strava_start_date(response_json["start_date"])
        if converted_date > last_updated_warehouse:
            activity = parse_api_output(response_json)
            all_activities.append(activity)
            activity_num += 1
        else:
           break
        
    return all_activities

def add_new_activities_to_db(activities:List):
    """Add the most recent activities to the local db"""
    
    #Turn the activities into a dataframe and clean them before adding to the db
    df = clean_data(pd.DataFrame(activities))
    add_to_db(df)

if __name__ == '__main__':
    main()
