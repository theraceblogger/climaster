## This script fetches data from the stations database, adds a two-letter country code
## and saves as stations.csv
import os
import psycopg2
from psycopg2.extras import DictCursor
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry import MultiPoint
import reverse_geocoder
import geopandas as gpd
from shapely.geometry import Point, Polygon


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

# cur = db_connect()


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


def reduce_df(df):
    print(f'Original number of stations: {len(df)}')
    coords = df[['latitude', 'longitude']].to_numpy()
    kms_per_radian = 6371.0088
    epsilon = 1.5 / kms_per_radian
    db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
    cluster_labels = db.labels_
    num_clusters = len(set(cluster_labels))
    clusters = pd.Series([coords[cluster_labels == n] for n in range(num_clusters)])
    print(f'Number of clusters: {num_clusters}')



def fetch_stations():
    # query = 'SELECT sr.station_jsonb FROM weather.stations_raw sr'
    query = 'SELECT sr.station_jsonb\
        FROM weather.stations_raw sr\
            WHERE (sr.station_jsonb ->> \'maxdate\')::date >= CURRENT_DATE - INTERVAL \'10 years\'\
                AND (sr.station_jsonb ->> \'maxdate\')::date - INTERVAL \'30 years\' >= (sr.station_jsonb ->> \'mindate\')::date'
    cur.execute(query)
    results = cur.fetchall()
    
    flat_results = []
    for result in results:
        flat_results.append(result[0])
    
    df = pd.DataFrame(flat_results)
    # df_cc = add_cc(df)
    df_reduce = reduce_df(df)

    # df_cc.to_csv('/home/theraceblogger/weather-db-example/stations.csv', index=False)


# fetch_stations()



## clustering algorithm to reduce number of weather stations
df = pd.read_csv('/Users/chuckschultz/climaster/CSVs/stations_10active_30span.csv')
# reduce_df(df)
print(f'Original number of stations: {len(df)}')
coords = df[['latitude', 'longitude']].to_numpy()
kms_per_radian = 6371.0088
epsilon = 35 / kms_per_radian
db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
cluster_labels = db.labels_
num_clusters = len(set(cluster_labels))
clusters = pd.Series([coords[cluster_labels == n] for n in range(num_clusters)])
print(f'Number of clusters: {num_clusters}')


def get_centermost_point(cluster):
    centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
    centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
    return tuple(centermost_point)
centermost_points = clusters.map(get_centermost_point)

lats, lons = zip(*centermost_points)
rep_points = pd.DataFrame({'lon':lons, 'lat':lats})

rs = rep_points.apply(lambda row: df[(df['latitude']==row['lat']) & (df['longitude']==row['lon'])].iloc[0], axis=1)

map = gpd.read_file('/Users/chuckschultz/climaster/plot_data/gadm36_shp/gadm36.shp')
lats = rs.latitude.to_list()
lons = rs.longitude.to_list()
coords = list(zip(lons, lats))
geometry = [Point(xy) for xy in coords]
geo_df = gpd.GeoDataFrame(rs, 
                          crs='EPSG:4326', 
                          geometry = geometry)

fig, ax = plt.subplots(figsize=(20,10))
map.to_crs(epsg=4326).plot(ax=ax, color='lightgrey')
geo_df.plot(ax=ax, alpha=0.5, markersize=0.3)
ax.set_title('Weather Stations\n10_active_30span_limit')
fig.savefig("stations_10active_30span_limit.png")
plt.show()
