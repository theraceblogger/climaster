## This script gets data from NOAA, of stations in weather.stations_to_load, and stores it in weather.weather_raw.
## Updates weather.stations_loaded. Need to manually TRUNCATE weather.stations_loaded.
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
datatype = "TMIN,TMAX,PRCP,SNOW,SNWD"
station_id = "&stationid="
start_date = "&startdate="
end_date = "&enddate="
units = "&units=standard"
limit = "&limit=1000"
offset = "&offset="


# Function gets data by customizing the iterations from the metadata (years:1950+) and calling load_data()
def get_data(station):
    query = f"SELECT sl.station_jsonb ->> 'mindate', sl.station_jsonb ->> 'maxdate', sl.country_code, sl.region FROM weather.stations_to_load sl WHERE sl.station_id = '{station}'"
    cur.execute(query)
    meta = cur.fetchall()
    print(meta)
    country, area = meta[0][2], meta[0][3]
    start, end = meta[0][0], meta[0][1]
    start_yr, end_yr = start[:4], end[:4]
    if int(start_yr) < 1950:
        start_yr = '1950'
        start = '1950-01-01'
    
    num_years = int(end_yr) - int(start_yr) +1

    for year in range(num_years):
        if num_years == 1:
            url = base_url + dataset_id + datatype_id + datatype + station_id + station + start_date + start + end_date + end + units + limit + offset
            load_data(url, country, area)

        elif year == 0:
            url = base_url + dataset_id + datatype_id + datatype + station_id + station + start_date + start + end_date + start_yr + "-12-31" + units + limit + offset
            load_data(url, country, area)

        elif year == num_years - 1:
            url = base_url + dataset_id + datatype_id + datatype + station_id + station + start_date + end_yr + "-01-01" + end_date + end + units + limit + offset
            load_data(url, country, area)

        else:
            url = base_url + dataset_id + datatype_id + datatype + station_id + station + start_date + str(int(start_yr) + year) + "-01-01" + end_date + str(int(start_yr) + year) + "-12-31" + units + limit + offset
            load_data(url, country, area)


# Function gets the data and inserts it into weather.weather_raw, 1000 at a time
def load_data(url, country, area, off_set=1):
    try:
        url2 = url + str(off_set)
        # time.sleep(10)
        r = requests.get(url2, headers=header)

        if r.status_code == 200:
            j = r.json()
            for result in j['results']:
                print(result['station'], result['date'], result['datatype'])
                print(country, area)
                # try:
                #     insert_sql = "INSERT INTO weather.weather_raw (station_id, date, datatype, country_code, region, weather_jsonb) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (station_id, date, datatype) DO UPDATE SET country = %s, region = %s, weather_jsonb = %s"
                #     cur.execute(insert_sql, (result['station'], result['date'], result['datatype'], country, area, json.dumps(result, indent=4, sort_keys=True), country, area, json.dumps(result, indent=4, sort_keys=True)))
                # except:
                #     print ('could not iterate through results')
                sys.exit()
            off_set += 1000
            if (off_set <= j['metadata']['resultset']['count']):
                load_data(url, country, area, off_set)
        elif r.status_code == 429:
            sys.exit('Too many API calls!')
        else:
            print(r.status_code)
    except KeyError:
        pass


# Main function - calls get_data() for stations not loaded and updates weather.stations_loaded (Must manually TRUNCATE)
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

    # get weather data and load into weather.weather_raw
    for station in stations:
        if station in stations_loaded:
            continue
        else:
            get_data(station)
            # update stations_loaded
            try:
                insert_sql = "INSERT INTO weather.stations_loaded (station_id) VALUES (%s) ON CONFLICT (station_id) DO UPDATE SET station_id = %s"
                cur.execute(insert_sql, (station, station))
            except:
                print ('could not update stations_loaded')

get_weather()
