## This script gets data from NOAA, and stores it in weather.noaa_raw
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

# clustering algorithm
def cluster_stations(df, radius):
    coords = df[['latitude', 'longitude']].to_numpy()
    kms_per_radian = 6371.0088
    epsilon = radius / kms_per_radian
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


query = "SELECT sr.station_jsonb\
    FROM weather.stations_raw sr\
        WHERE (sr.station_jsonb ->> 'maxdate')::date >= CURRENT_DATE - INTERVAL '1 years'\
            AND (sr.station_jsonb ->> 'maxdate')::date - INTERVAL '30 years' >= (sr.station_jsonb ->> 'mindate')::date"
cur.execute(query)
results = cur.fetchall()

flat_results = []
for result in results:
    flat_results.append(result[0])
df = pd.DataFrame(flat_results)

print(f"Original number of stations: {len(df)}")
radii = [5, 25, 50, 75]
for iteration, radius in enumerate(radii):
    clusters = cluster_stations(df, radius)
    print(f"Number of stations after {iteration + 1} iteration(s): {len(clusters)}")
    if iteration < 2:
        df = get_highest_coverage_station(clusters, df)
    else:
        centermost_points = clusters.map(get_centermost_point)
        lats, lons = zip(*centermost_points)
        rep_points = pd.DataFrame({'lat':lats, 'lon':lons})
        df = rep_points.apply(lambda row: df[(df['latitude']==row['lat']) & (df['longitude']==row['lon'])].iloc[0], axis=1)
















df.to_csv('/home/ec2-user/climaster/stations_world_reduce.csv', index=False)