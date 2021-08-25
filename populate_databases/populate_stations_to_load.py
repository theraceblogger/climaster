## This script gets station data from weather.stations_raw,
## selects current stations (within 1 year) with 60 years of data,
## uses a clustering algorithm to spatially reduce the stations to 1 per 500 km radius, 
## adds the two letter country code, and stores the stations in weather.stations_world
import os
import psycopg2
from psycopg2.extras import DictCursor
import json
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
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



## iso
iso_dict = {
    # Asia
    'southern_asia':['AF', 'BD', 'BT', 'IN', 'IR', 'LK', 'MV', 'NP', 'PK'],
    'western_asia':['AE', 'AM', 'AZ', 'BH', 'CY', 'GE', 'IQ', 'IL', 'JO', 'KW', 'LB', 'OM', 'PS', 'QA', 'SA', 'SY', 'TR', 'YE'],
    'southeastern_asia':['BN', 'ID', 'KH', 'LA', 'MM', 'MY', 'PH', 'SG', 'TH', 'VN', 'TL'],
    'eastern_asia':['CN', 'HK', 'JP', 'KR', 'MO', 'MN', 'KP', 'TW'],
    'central_asia':['KZ', 'KG', 'TJ', 'TM', 'UZ'],
    # Africa
    'middle_africa':['AO', 'CF', 'CM', 'CG', 'GA', 'GQ', 'ST', 'TD', 'CD'],
    'eastern_africa':['BI', 'KM', 'DJ', 'ER', 'ET', 'KE', 'MG', 'MZ', 'MU', 'MW', 'RE', 'RW', 'SO', 'SC', 'TZ', 'UG', 'ZM', 'ZW',\
        'YT', 'TF', 'IO'],
    'western_africa':['BJ', 'BF', 'CI', 'CV', 'GH', 'GN', 'GM', 'GW', 'LR', 'ML', 'MR', 'NE', 'NG', 'SN', 'SH', 'SL', 'TG'],
    'southern_africa':['BW', 'LS', 'NA', 'SZ', 'ZA'],
    'northern_africa':['DZ', 'EG', 'EH', 'LY', 'MA', 'SD', 'TN', 'SS'],
    # Americas
    'caribbean':['AI', 'CW', 'AG', 'BS', 'BB', 'CU', 'KY', 'DM', 'DO', 'GP', 'GD', 'HT', 'JM', 'KN', 'LC', 'MS', 'MQ', 'PR', 'TC',\
        'TT', 'VC', 'VG', 'VI', 'AW', 'BQ', 'BL', 'CW', 'MF', 'SX'],
    'south_america':['AR', 'BO', 'BR', 'CL', 'CO', 'EC', 'FK', 'GF', 'GY', 'PE', 'PY', 'SR', 'UY', 'VE', 'BV', 'GS'],
    'central_america':['BZ', 'CR', 'GT', 'HN', 'MX', 'NI', 'PA', 'SV'],
    'northern_america':['BM', 'CA', 'GL', 'PM', 'US'],
    # Europe
    'southern_europe':['AL', 'AD', 'PT', 'BA', 'ES', 'GI', 'GR', 'HR', 'IT', 'MK', 'MT', 'PT', 'SM', 'RS', 'SI', 'VA', 'ME'],
    'western_europe':['AT', 'BE', 'CH', 'DE', 'FR', 'LI', 'LU', 'MC', 'NL'],
    'eastern_europe':['BG', 'BY', 'CZ', 'HU', 'MD', 'PL', 'RO', 'RU', 'SK', 'UA'],
    'northern_europe':['DK', 'EE', 'FI', 'FO', 'GB', 'IE', 'IS', 'LT', 'LV', 'IM', 'NO', 'SE', 'SJ', 'AX', 'GG', 'JE'],
    'russian_federation':['RU'],
    # Oceania
    'polynesia':['AS', 'CK', 'NU', 'PN', 'PF', 'TK', 'TO', 'TV', 'WF', 'WS'],
    'australia_new_zealand':['AU', 'NZ', 'NF', 'CC', 'CX', 'HM'],
    'melanesia':['FJ', 'NC', 'PG', 'SB', 'VU'],
    'micronesia':['FM', 'GU', 'KI', 'MH', 'NR', 'PW', 'MP', 'UM']}

# add country_code and region to dataframe
def add_cc_region(df):
    country, region = [], []
    lats = df.latitude.to_list()
    lons = df.longitude.to_list()
    coords = list(zip(lats, lons))

    for i in range(len(coords)):
        location = reverse_geocoder.search(coords[i])
        country.append(location[0]['cc'])
        for key, value in iso_dict.items():
            if location[0]['cc'] in value:
                region.append(key)
                break
        else:
            print(location[0]['cc'])
    df['cc'] = country
    df['region'] = region
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



# get stations (data within 1 year) with 60 years of data and loads into weather.stations_world
def get_stations():
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

    # use clustering algorithm and choose station with highest coverage
    radii = [5, 25, 50, 75, 100, 150, 200, 300, 400, 500]
    for radius in radii:
        clusters = cluster_stations(df, radius)
        df = get_highest_coverage_station(clusters, df)

    # add country code
    df = add_cc_region(df)

    # load into stations_world
    j = df.to_json(orient='records')
    results = json.loads(j)

    for result in results:
        try:
            insert_sql = "INSERT INTO weather.stations_to_load (station_id, country_code, region, station_jsonb) VALUES (%s,%s,%s,%s) ON CONFLICT (station_id) DO UPDATE SET country_code = %s, region = %s, station_jsonb = %s"
            cur.execute(insert_sql, (result['id'], result['cc'], result['region'], json.dumps(result, indent=4, sort_keys=True), result['cc'], result['region'], json.dumps(result, indent=4, sort_keys=True))) 
        except:
            print ('could not iterate through results')

get_stations()