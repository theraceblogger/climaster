## This script gets stations data from NOAA, reduces stations with a clustering algorithm (DBSCAN),
## adds the 2 letter country code, and stores it in weather.stations_raw_limit
import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
import json
import time
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry import MultiPoint
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


# add country code to dataframe
def add_cc(df):
    country = []
    lats = df.latitude.to_list()
    lons = df.longitude.to_list()
    coords = list(zip(lats, lons))

    for i in range(len(coords)):
        location = reverse_geocoder.search(coords[i])
        country.append(location[0]['cc'])
    df['cc'] = country
    return df


# clustering algorithm
def cluster_stations(stations):    
    coords = stations[['latitude', 'longitude']].to_numpy()
    kms_per_radian = 6371.0088
    epsilon = 35 / kms_per_radian
    db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
    cluster_labels = db.labels_
    num_clusters = len(set(cluster_labels))
    clusters = pd.Series([coords[cluster_labels == n] for n in range(num_clusters)])
    return clusters


# choose point in cluster closest to centroid
def get_centermost_point(cluster):
    centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
    centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
    return tuple(centermost_point)
# centermost_points = clusters.map(get_centermost_point)


# choose point in cluster with highest coverage
def get_highest_coverage_station(clusters, stations):
    points = pd.DataFrame()
    for cluster in clusters:
        lats, lons = zip(*cluster)
        cluster_df = pd.DataFrame({'lat':lats, 'lon':lons})
        cluster_df = cluster_df.apply(lambda row: stations[(stations['latitude']==row['lat']) & (stations['longitude']==row['lon'])].iloc[0], axis=1)
        chosen = cluster_df[cluster_df.datacoverage == cluster_df.datacoverage.max()]
        points = points.append(chosen.head(1), ignore_index=True, sort=False)
    return points



def get_stations():
    query = 'SELECT sr.station_jsonb\
        FROM weather.stations_raw sr\
            WHERE (sr.station_jsonb ->> \'maxdate\')::date >= CURRENT_DATE - INTERVAL \'10 years\'\
                AND (sr.station_jsonb ->> \'maxdate\')::date - INTERVAL \'30 years\' >= (sr.station_jsonb ->> \'mindate\')::date'
    cur.execute(query)
    results = cur.fetchall()
    
    flat_results = []
    for result in results:
        flat_results.append(result[0])
    stations = pd.DataFrame(flat_results)

    clusters = cluster_stations(stations)
    df = get_highest_coverage_station(clusters, stations)
    df = add_cc(df)
    results = df.to_dict('records')

    for result in results:
        try:
            insert_sql = "INSERT INTO weather.stations_raw_limit (station_id, station_jsonb) VALUES (%s,%s) ON CONFLICT (station_id) DO UPDATE SET station_jsonb = %s"
            cur.execute(insert_sql, (result['id'], json.dumps(result, indent=4, sort_keys=True), json.dumps(result, indent=4, sort_keys=True))) 
        except:
            print ('could not iterate through results')


get_stations()