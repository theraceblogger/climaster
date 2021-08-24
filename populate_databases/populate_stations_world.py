## This script gets station data from stations_raw, selects current stations (within 1 year),
## uses a clustering algorithm to spatially reduce the stations to 1 per 500 km radius, 
## adds the two letter country code, and stores the stations in stations_world
import os
import psycopg2
from psycopg2.extras import DictCursor
import json
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
# from geopy.distance import great_circle
# from shapely.geometry import MultiPoint
import reverse_geocoder

# import geopandas as gpd
# import matplotlib.pyplot as plt
# from shapely.geometry import Point, Polygon


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

# get stations (data within 1 year) with 120 years of data
query = "SELECT sr.station_jsonb\
    FROM weather.stations_raw sr\
        WHERE (sr.station_jsonb ->> 'maxdate')::date >= CURRENT_DATE - INTERVAL '1 years'\
            AND (sr.station_jsonb ->> 'maxdate')::date - INTERVAL '60 years' >= (sr.station_jsonb ->> 'mindate')::date"
cur.execute(query)
results = cur.fetchall()

flat_results = []
for result in results:
    flat_results.append(result[0])
df = pd.DataFrame(flat_results)
# print(f'Starting with {len(df)} stations')

# use clustering algorithm and choose station with highest coverage
radii = [5, 25, 50, 75, 100, 150, 200, 300, 400, 500]
for radius in radii:
    clusters = cluster_stations(df, radius)
    df = get_highest_coverage_station(clusters, df)
    # print(f'\nRadius: {radius}, Number of Stations: {len(df)}\nCoverage: {df.datacoverage.mean():.3f}')

# add country code
df = add_cc(df)

# map = gpd.read_file('/Users/chuckschultz/climate_disaster_project/climaster_unused/plot_data/gadm36_shp/gadm36.shp')

# lats = df.latitude.to_list()
# lons = df.longitude.to_list()
# lats = df.iloc[:,0].to_list()
# lons = df.iloc[:,1].to_list()
# coords = list(zip(lons, lats))

# geometry = [Point(xy) for xy in coords]
# geo_df = gpd.GeoDataFrame(df, 
#                           crs='EPSG:4326', 
#                           geometry = geometry)

# fig, ax = plt.subplots(figsize=(20,10))
# map.to_crs(epsg=4326).plot(ax=ax, color='lightgrey')
# geo_df.plot(ax=ax, alpha=0.7, markersize=0.7)
# ax.set_title('Weather Stations\n60 Years - Cluster500km')
# fig.savefig("stations_world_60_cluster500km.png")

# load into stations_world
j = df.to_json(orient='records')
results = json.loads(j)

for result in results:
    try:
        insert_sql = "INSERT INTO weather.stations_world (station_id, country_code, station_jsonb) VALUES (%s,%s,%s) ON CONFLICT (station_id) DO UPDATE SET country_code = %s, station_jsonb = %s"
        cur.execute(insert_sql, (result['id'], result['cc'], json.dumps(result, indent=4, sort_keys=True), result['cc'], json.dumps(result, indent=4, sort_keys=True))) 
    except:
        print ('could not iterate through results')

