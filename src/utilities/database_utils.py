import pyodbc
import configparser
import pandas as pd

from sqlalchemy import create_engine
from datetime import datetime

def conn_string() -> str:

    parser = configparser.ConfigParser()
    parser.read("config.ini")
    
    driver = parser.get("DATABASE", "DRIVER")
    server = parser.get("DATABASE", "SERVER")
    dbname = parser.get("DATABASE", "DB_NAME")
    
    return f"Driver={driver};Server={server};Database={dbname};Trusted_Connection=yes;"

def connect_sql_server():

    conn = pyodbc.connect(conn_string())

    if conn is None:
        print("Error connecting")
    else:
        print("Connection established!")
    return conn

def get_columns_dictionary() -> dict:
    col_dict = {
        'id' : 'int64',
        'name' : 'object',
        'distance' : 'float64',
        'moving_time' : 'float64',
        'elapsed_time' : 'float64',
        'total_elevation_gain' : 'float64',
        'type' : 'object',
        'workout_type' : 'object',
        'location_country' : 'object',
        'achievement_count' : 'int64',
        'kudos_count' : 'int64',
        'comment_count' : 'int64',
        'athlete_count' : 'int64',
        'photo_count' : 'int64',
        'average_speed' : 'float64',
        'max_speed' : 'float64',
        'average_cadence' : 'float64',
        'average_temp' : 'float64',
        'average_heartrate' : 'float64',
        'max_heartrate' : 'int64',
        'suffer_score' : 'int64',
        'start_date' : 'datetime64[ns]',
        'timezone' : 'object',
        'lat' : 'float64',
        'lng' : 'float64'
    }
    return col_dict

def add_to_db(df:pd.DataFrame):
    try:
        parser = configparser.ConfigParser()
        parser.read("config.ini")
    
        driver = parser.get("DATABASE", "DRIVER")
        server = parser.get("DATABASE", "SERVER")
        dbname = parser.get("DATABASE", "DB_NAME")

        connection_url = f"mssql+pyodbc://@{server}/{dbname}?driver={driver.replace(' ', '+').replace('{','').replace('}','')}&trusted_connection=yes"
        engine = create_engine(connection_url)
        df.to_sql('strava_activity', con=engine, if_exists='replace', index=False)
        print(f"success: {df.shape[0]} rows added")
    except:
        print(f"fail! Something went wrong")
