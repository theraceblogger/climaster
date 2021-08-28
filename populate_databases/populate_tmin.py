'''This script gets minimum temperatures for stations_to_load and loads them into weather_tmin,
updates stations_loaded'''

import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
import json
import time
import sys

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
datatype_id = "&datatypeid="
datatype = "TMIN"
station_id = "&stationid="
start_date = "&startdate="
end_date = "&enddate="
units = "&units=standard"
limit = "&limit=1000"
offset = "&offset="

# Function gets data by customizing the iterations from the metadata (years:1950+) and calling load_data()
def get_data(station, api_calls):
    query = f"SELECT sl.station_jsonb ->> 'mindate', sl.station_jsonb ->> 'maxdate', sl.country_code, sl.region FROM weather.stations_to_load sl WHERE sl.station_id = '{station}'"
    cur.execute(query)
    meta = cur.fetchall()
    
    country, region = meta[0][2], meta[0][3]
    start, end = meta[0][0], meta[0][1]
    start_yr, end_yr = start[:4], end[:4]
    if int(start_yr) < 1950:
        start_yr = '1950'
        start = '1950-01-01'
    
    num_years = int(end_yr) - int(start_yr) +1

    for year in range(num_years):
        if num_years == 1:
            url = base_url + dataset_id + datatype_id + datatype + station_id + station + start_date + start + end_date + end + units + limit + offset
            api_calls = load_data(url, country, region, api_calls)

        elif year == 0:
            url = base_url + dataset_id + datatype_id + datatype + station_id + station + start_date + start + end_date + start_yr + "-12-31" + units + limit + offset
            api_calls = load_data(url, country, region, api_calls)

        elif year == num_years - 1:
            url = base_url + dataset_id + datatype_id + datatype + station_id + station + start_date + end_yr + "-01-01" + end_date + end + units + limit + offset
            api_calls = load_data(url, country, region, api_calls)

        else:
            url = base_url + dataset_id + datatype_id + datatype + station_id + station + start_date + str(int(start_yr) + year) + "-01-01" + end_date + str(int(start_yr) + year) + "-12-31" + units + limit + offset
            api_calls = load_data(url, country, region, api_calls)
    return api_calls

# Function gets the data and inserts it into weather_tmin, 1000 at a time
def load_data(url, country, region, api_calls, off_set=1):
    try:
        url2 = url + str(off_set)
        time.sleep(.2)
        r = requests.get(url2, headers=header)
        api_calls += 1

        if r.status_code == 200:
            j = r.json()
            for result in j['results']:
                try:
                    insert_sql = "INSERT INTO weather.weather_tmin (station_id, date, country_code, region, weather_jsonb) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (station_id, date) DO UPDATE SET country_code=%s, region=%s, weather_jsonb=%s"
                    cur.execute(insert_sql, (result['station'], result['date'], country, region, json.dumps(result, indent=4, sort_keys=True), country, region, json.dumps(result, indent=4, sort_keys=True)))
                except:
                    print ('could not iterate through results')
                
            off_set += 1000
            if (off_set <= j['metadata']['resultset']['count']):
                api_calls = load_data(url, country, region, api_calls, off_set)
        elif r.status_code == 429:
            sys.exit(f'Too many API calls!\n{api_calls} requests')
        else:
            print('\nError Code:', r.status_code)
            print(url2)
    except KeyError:
        pass
    return api_calls

# Main function - calls get_data() for stations_to_load and updates stations_loaded (Must manually TRUNCATE)
def get_weather():
    # get list of stations
    query = "SELECT station_id FROM weather.stations_to_load"
    cur.execute(query)
    results = cur.fetchall()

    stations = []
    for result in results:
        stations.append(result[0])

    # get list of stations loaded
    query = "SELECT station_id FROM weather.stations_loaded"
    cur.execute(query)
    results = cur.fetchall()

    stations_loaded = []
    for result in results:
        stations_loaded.append(result[0])

    # get weather data and load into weather_tmin
    api_calls = 0
    for station in stations[:2]:
        if station in stations_loaded:
            continue
        else:
            api_calls = get_data(station, api_calls)
            # update stations_loaded
            try:
                insert_sql = "INSERT INTO weather.stations_loaded (station_id) VALUES (%s) ON CONFLICT (station_id) DO UPDATE SET station_id = %s"
                cur.execute(insert_sql, (station, station))
            except:
                print ('could not update stations_loaded')
    return api_calls

api_calls = get_weather()
print(f'{api_calls} requests')