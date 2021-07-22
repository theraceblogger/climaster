## This script fetches data from the disasters database, adds the two_letter country code
## and saves as disasters.csv file
import os
import psycopg2
from psycopg2.extras import DictCursor
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

cc = []
def add_cc(df):
    for i in range(df.shape[0]):
        try:
            iso = country_alpha3_to_country_alpha2(df.iloc[i].ISO)
            cc.append(iso)
        except KeyError:
            cc.append(extras_dict[df.iloc[i].ISO])
    df['cc'] = cc
    return df


def fetch_disasters():
    query = 'SELECT er.emdat_jsonb FROM weather.emdat_raw er'
    cur.execute(query)
    results = cur.fetchall()
    
    flat_results = []
    for result in results:
        flat_results.append(result[0])
    
    df = pd.DataFrame(flat_results)
    df_cc = add_cc(df)
    df_cc.to_csv('/home/theraceblogger/weather-db-example/disasters.csv', index=False)


fetch_disasters()

