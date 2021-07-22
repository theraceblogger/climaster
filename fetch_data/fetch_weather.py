## This script fetches data from the weather database and saves as a csv file
import os
import psycopg2
from psycopg2.extras import DictCursor
import pandas as pd

# Function that connects to database
def db_connect():
    db_name = os.environ['db_name']
    db_user = os.environ['db_user']
    db_host = os.environ['db_host']
    db_credentials = os.environ['db_creds']
  
    conn_string = "dbname='" + str(db_name) + "' user='" + str(db_user) + "' host='" + str(db_host) + "' password='" + str(db_credentials) + "'"

    try:
        conn = psycopg2.connect(str(conn_string))
        conn.autocommit = True
    except:
        print("Unable to connect to the database")

    cur = conn.cursor(cursor_factory=DictCursor)
    return cur

cur = db_connect()


def fetch_weather(station_id):
    query = f'SELECT nr.noaa_jsonb FROM weather.noaa_raw nr WHERE nr.station_id = \'GHCND:{station_id}\''
    cur.execute(query)
    results = cur.fetchall()
    
    flat_results = []
    for result in results:
        flat_results.append(result[0])
    
    df = pd.DataFrame(flat_results)
    df.to_csv(f'/home/theraceblogger/weather-db-example/weather_{station_id}.csv', index=False)


station_id = 'AFM00040948'
fetch_weather(station_id)

