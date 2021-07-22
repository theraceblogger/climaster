## This script gets data from NOAA, and stores it in weather.noaa_raw
import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
from datetime import datetime
import json
import time
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


# Set variables
noaa_token = os.environ['noaa_token']
header = {'token': noaa_token}
base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
dataset_id = "?datasetid=GHCND"
station_id = "&stationid="
start_date = "&startdate="
end_date = "&enddate="
limit = "&limit=1000"
offset = "&offset="

# Function gets weather station id, mindate and maxdate
# Calls get_data() for each weather station's data
def get_meta():
    query = "SELECT sr.station_id, sr.station_jsonb ->> 'mindate', sr.station_jsonb ->> 'maxdate' FROM weather.stations_raw sr"
    cur.execute(query)
    return cur.fetchall()
    #print(len(results))
    # for result in results:
    #     get_data(result)

# Function gets data by customizing the iterations from the metadata and calling load_data()
def get_data(result): # result is a list of strings
    station, start, end = result[0], result[1], result[2]
    start_yr, end_yr = start[:4], end[:4]
    num_years = int(end_yr) - int(start_yr) +1

    for year in range(num_years):
        if num_years == 1:
            url = base_url + dataset_id + station_id + station + start_date + start + end_date + end + limit + offset
            load_data(url)

        elif year == 0:
            url = base_url + dataset_id + station_id + station + start_date + start + end_date + start_yr + "-12-31" + limit + offset
            load_data(url)

        elif year == num_years - 1:
            url = base_url + dataset_id + station_id + station + start_date + end_yr + "-01-01" + end_date + end + limit + offset
            load_data(url)

        else:
            url = base_url + dataset_id + station_id + station + start_date + str(int(start_yr) + year) + "-01-01" + end_date + str(int(start_yr) + year) + "-12-31" + limit + offset
            load_data(url)

# Function gets the data and inserts it into the database, 1000 at a time
def load_data(url, off_set=1):
    try:
        url2 = url + str(off_set)
        time.sleep(1)
        r = requests.get(url2, headers=header)
        j = r.json()
        for result in j['results']:
            try:
                insert_sql = "INSERT INTO weather.noaa_raw (station_id, date, data_type, noaa_jsonb) VALUES (%s,%s,%s,%s) ON CONFLICT (station_id, date, data_type) DO UPDATE SET noaa_jsonb = %s"
                cur.execute(insert_sql, (result['station'], result['date'], result['datatype'], json.dumps(result, indent=4, sort_keys=True), json.dumps(result, indent=4, sort_keys=True)))
            except:
                print ('could not iterate through results')
        off_set += 1000
        if (off_set <= j['metadata']['resultset']['count']):
            load_data(url, off_set)
    except KeyError:
        pass



results = get_meta()

loaded = pd.read_csv('/home/theraceblogger/weather-db-example/loaded.csv')
loaded = loaded['0'].tolist()

print("Number of stations loaded:", len(loaded))
#loaded = []

def iter_result():
    for i in range(len(loaded), len(results)):
        try:
            get_data(results[i])
            loaded.append(results[i][0])
        except Exception as error:
            loaded_df = pd.DataFrame(loaded)
            loaded_df.to_csv('/home/theraceblogger/weather-db-example/loaded.csv', index=False)
            raise error

iter_result()