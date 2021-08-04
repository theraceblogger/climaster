## This script gets data from NOAA, and stores it in weather_raw
import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
import json
import time


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


# Function gets data by customizing the iterations from the metadata and calling load_data()
def get_data(station):
    query = f"SELECT sw.station_id, srl.station_jsonb ->> 'mindate', sw.station_jsonb ->> 'maxdate' FROM weather.stations_world sw WHERE sw.station_id = '{station}'"
    cur.execute(query)
    meta = cur.fetchall()
    start, end = meta[0][0], meta[0][1]
    start_yr, end_yr = start[:4], end[:4]
    if int(start_yr) < 1990:
        start_yr = '1990'
        start = '1990-01-01'
    num_years = int(end_yr) - int(start_yr) +1

    for year in range(num_years):
        if num_years == 1:
            url = base_url + dataset_id + datatype_id + datatype + station_id + station + start_date + start + end_date + end + units + limit + offset
            load_data(url)

        elif year == 0:
            url = base_url + dataset_id + datatype_id + datatype + station_id + station + start_date + start + end_date + start_yr + "-12-31" + units + limit + offset
            load_data(url)

        elif year == num_years - 1:
            url = base_url + dataset_id + datatype_id + datatype + station_id + station + start_date + end_yr + "-01-01" + end_date + end + units + limit + offset
            load_data(url)

        else:
            url = base_url + dataset_id + datatype_id + datatype + station_id + station + start_date + str(int(start_yr) + year) + "-01-01" + end_date + str(int(start_yr) + year) + "-12-31" + units + limit + offset
            load_data(url)


# Function gets the data and inserts it into the database, 1000 at a time
def load_data(url, off_set=1):
    try:
        url2 = url + str(off_set)
        time.sleep(10)
        r = requests.get(url2, headers=header)
        j = r.json()
        for result in j['results']:
            try:
                insert_sql = "INSERT INTO weather.weather_raw (station_id, date, datatype, weather_jsonb) VALUES (%s,%s,%s,%s) ON CONFLICT (station_id, date, datatype) DO UPDATE SET weather_jsonb = %s"
                cur.execute(insert_sql, (result['station'], result['date'], result['datatype'], json.dumps(result, indent=4, sort_keys=True), json.dumps(result, indent=4, sort_keys=True)))
            except:
                print ('could not iterate through results')
        off_set += 1000
        if (off_set <= j['metadata']['resultset']['count']):
            load_data(url, off_set)
    except KeyError:
        pass


# get list of stations
query = "SELECT station_id FROM weather.stations_world"
cur.execute(query)
results = cur.fetchall()

stations = []
for result in results:
    stations.append(result[0])

# get number of stations loaded for index
query = "SELECT COUNT(*) FROM weather.stations_loaded"
cur.execute(query)
count = cur.fetchall()
begin_index = count[0]

# get weather data and load into table weather_raw
for i in range(begin_index, len(stations)):
    station = stations[i]
    get_data(station)
    # update stations_loaded
    try:
        insert_sql = "INSERT INTO weather.stations_loaded (station_id) VALUES (%s) ON CONFLICT (station_id) DO UPDATE SET station_id = %s"
        cur.execute(insert_sql, (station, station))
    except:
        print ('could not update stations_loaded')