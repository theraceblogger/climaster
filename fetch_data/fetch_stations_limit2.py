## This script fetches data from the stations_raw_limit database
## and saves as stations_limit.csv
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



def fetch_stations():
    query = 'SELECT src.station_jsonb FROM weather.stations_raw_limit src'
    cur.execute(query)
    results = cur.fetchall()
    
    flat_results = []
    for result in results:
        flat_results.append(result[0])
    
    df = pd.DataFrame(flat_results)
    df.to_csv('/home/ec2-user/climaster/stations_limit.csv', index=False)


fetch_stations()