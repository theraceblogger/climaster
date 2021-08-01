## This script gets data from NOAA, and stores it in weather.noaa_raw
import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
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


# datatypes = ['TAVG', 'TMIN', 'TMAX', 'PRCP', 'SNOW']

def create_table(country):
    # query = f"DROP TABLE weather.{country}_{datatype}"
    # cur.execute(query)
    query = f"CREATE TABLE weather.{country} (station_id varchar(255) NOT NULL, date varchar(255) NOT NULL, datatype varchar(255) NOT NULL, value int, attributes varchar(255), CONSTRAINT PK_{country} PRIMARY KEY (station_id, date, datatype))"
    cur.execute(query)


# Function gets the data and inserts it into the database, 1000 at a time
def load_data(url, country, off_set=1):
    try:
        url2 = url + str(off_set)
        time.sleep(10)
        r = requests.get(url2, headers=header)
        j = r.json()
        for result in j['results']:
            try:
                insert_sql = f"INSERT INTO weather.{country} (station_id, date, data_type, value, attributes) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (station_id, date, data_type) DO UPDATE SET value = %s, attributes = %s"
                cur.execute(insert_sql, (result['station'], result['date'], result['datatype'], result['value'], result['attributes'], result['value'], result['attributes']))
            except:
                print ('could not iterate through results')
        off_set += 1000
        if (off_set <= j['metadata']['resultset']['count']):
            load_data(url, country, off_set)
    except KeyError:
        pass
    # except Exception as error:
    #     print(error)
    #     pass

# Function gets weather station id, mindate and maxdate
def get_meta(station):
    query = f"SELECT srl.station_jsonb ->> 'mindate', srl.station_jsonb ->> 'maxdate' FROM weather.stations_raw_limit srl WHERE srl.station_id = '{station}'"
    cur.execute(query)
    results = cur.fetchall()
    return results
        


# Set variables
noaa_token = os.environ['noaa_token']
header = {'token': noaa_token}
base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
dataset_id = "?datasetid=GHCND"
datatype_id = "&datatypeid="
datatype = "TMIN&TMAX&PRCP&SNOW&SNWD"
station_id = "&stationid="
start_date = "&startdate="
end_date = "&enddate="
limit = "&limit=1000"
offset = "&offset="


# query stations_by_country
# create_table for country
# get noaa data for each station and insert into tables
# track each station and country

query = "SELECT country, stations_jsonb, stations_count FROM weather.stations_by_country"
cur.execute(query)
results = cur.fetchall()

stations_loaded = {}
for result in results:
    stations_loaded[result[0]] = 0
    # create_table(result[0])
    for station in result[1]:
        meta = get_meta(station)
        print(meta)
    #     url = base_url + dataset_id + datatype_id + datatype + station_id + station + start_date + "1990-01-01" + end_date + "2020-12-31" + limit + offset
    #     load_data(url, result[0])
    # stations_loaded[result[0]] = stations_loaded[result[0]] + 1
    # insert_sql = f"INSERT INTO weather.stations_loaded (country, stations_loaded, stations_count) VALUES (%s,%s,%s) ON CONFLICT (country) DO UPDATE SET stations_loaded = %s, stations_count = %s"
    # cur.execute(insert_sql, (result[0], stations_loaded[result[0]], result[3], stations_loaded[result[0]], result[3]))
    break


















# Function gets weather station id, mindate and maxdate
# Calls get_data() for each weather station's data
# def get_meta():
#     query = "SELECT srl.station_id, srl.station_jsonb ->> 'mindate', srl.station_jsonb ->> 'maxdate' FROM weather.stations_raw_limit srl"
#     cur.execute(query)
#     results = cur.fetchall()
#     for result in results:
        # get_data(result)


# Function gets data by customizing the iterations from the metadata and calling load_data()
# def get_data(result): # result is a list of strings
#     station, start, end = result[0], result[1], result[2]
#     start_yr, end_yr = start[:4], end[:4]
#     num_years = int(end_yr) - int(start_yr) +1

#     for year in range(num_years):
#         if num_years == 1:
#             url = base_url + dataset_id + datatype_id + station_id + station + start_date + start + end_date + end + limit + offset
#             load_data(url)

#         elif year == 0:
#             url = base_url + dataset_id + datatype_id + station_id + station + start_date + start + end_date + start_yr + "-12-31" + limit + offset
#             load_data(url)

#         elif year == num_years - 1:
#             url = base_url + dataset_id + datatype_id + station_id + station + start_date + end_yr + "-01-01" + end_date + end + limit + offset
#             load_data(url)

#         else:
#             url = base_url + dataset_id + datatype_id + station_id + station + start_date + str(int(start_yr) + year) + "-01-01" + end_date + str(int(start_yr) + year) + "-12-31" + limit + offset
#             load_data(url)


# get_meta()
