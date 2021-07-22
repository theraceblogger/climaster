## This script fetches data from the stations database, adds a two-letter country code
## and saves as stations.csv
import os
import psycopg2
from psycopg2.extras import DictCursor
import pandas as pd
import reverse_geocoder

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


country = []
def add_cc(df):
    lats = df.latitude.to_list()
    lons = df.longitude.to_list()
    coords = list(zip(lats, lons))

    for i in range(len(coords)):
        location = reverse_geocoder.search(coords[i])
        country.append(location[0]['cc'])
    df['cc'] = country
    return df


def fetch_stations():
    query = 'SELECT sr.station_jsonb FROM weather.stations_raw sr'
    cur.execute(query)
    results = cur.fetchall()
    
    flat_results = []
    for result in results:
        flat_results.append(result[0])
    
    df = pd.DataFrame(flat_results)
    df_cc = add_cc(df)
    df_cc.to_csv('/home/theraceblogger/weather-db-example/stations.csv', index=False)


fetch_stations()


