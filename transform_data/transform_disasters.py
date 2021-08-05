import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
import json
import pandas as pd
import pycountry
from pycountry_convert import country_alpha3_to_country_alpha2


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


extras_dict = {
    "SUN":"RU",
    "YUG":"RS",
    "DFR":"DE",
    "SCG":"RS",
    "CSK":"CZ",
    "YMN":"YE",
    "SPI":"ES",
    "YMD":"YE",
    "AZO":"PT",
    "ANT":"CW",
    "DDR":"DE"}

def add_cc(df):
    cc = []
    for i in range(df.shape[0]):
        try:
            iso = country_alpha3_to_country_alpha2(df.iloc[i].ISO)
            cc.append(iso)
        except KeyError:
            cc.append(extras_dict[df.iloc[i].ISO])
    df['cc'] = cc
    return df


query = "SELECT dr.disaster_jsonb FROM weather.disasters_raw dr ORDER BY dr.disaster_jsonb ->> 'Year', dr.disaster_jsonb ->> 'Month', dr.disaster_jsonb ->> 'Day'"
cur.execute(query)
results = cur.fetchall()

flat_results = []
for result in results:
    flat_results.append(result[0])
df = pd.DataFrame(flat_results)

df = add_cc(df)

j = df.to_json(orient='records')
results = json.loads(j)

for result in results:
    try:
        insert_sql = "INSERT INTO weather.disasters_clean (disaster_no, year, country_code, region, disaster_type, deaths, damages)\
            VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (disaster_no) DO UPDATE SET year = %s, country_code = %s, region = %s, disaster_type = %s, deaths = %s, damages = %s"
        cur.execute(insert_sql, (result['Dis No'], result['Year'], result['cc'], result['Region'], result['Disaster Type'], result['Total Deaths'], result['Total Damages (\'000 US$)'], result['Year'], result['cc'], result['Region'], result['Disaster Type'], result['Total Deaths'], result['Total Damages (\'000 US$)']))
    except:
        print ('could not iterate through results')