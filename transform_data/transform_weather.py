'''This script takes the data in weather_raw, adds tavg (temperature average) and tanm (temperature anomaly),
averages each datatype per day, and stores in weather_full and weather_{region}'''

import os
import psycopg2
from psycopg2.extras import DictCursor
import json
import pandas as pd
from functools import reduce
from datetime import date


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


def create_table(region):
    query = f"CREATE TABLE IF NOT EXISTS weather.weather_{region}(date DATE NOT NULL PRIMARY KEY, tmin FLOAT, tmax FLOAT, tavg FLOAT, prcp FLOAT, snow FLOAT, snwd FLOAT, tanm FLOAT)"
    cur.execute(query)
    return

# Load weather.weather_full
def avg_daily_full():
    fields = ['TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD']
    dataframes = []

    for field in fields:
        query = f"SELECT (wr.date)::date date, AVG((wr.weather_jsonb ->> 'value')::decimal) {field}\
            FROM weather.weather_raw wr\
                WHERE wr.datatype = '{field}'\
                    GROUP BY wr.date\
                        ORDER BY wr.date"
        cur.execute(query)
        results = cur.fetchall()

        flat_results = [result for result in results]

        field = pd.DataFrame(flat_results, columns=['date', field])
        dataframes.append(field)
    
    # outer join field DataFrames, add TAVG and TANM
    df = reduce(lambda  left,right: pd.merge(left,right,on=['date'], how='outer'), dataframes)
    df['TAVG'] = df[['TMIN', 'TMAX']].mean(axis=1)
    mean = df[(df['date'] >= date(1951,1,1)) & (df['date'] <= date(1980,12,31))]['TAVG'].mean(axis=0)
    df['TANM'] = df['TAVG'] - mean

    # load results into weather.weather_full
    j = df.to_json(orient='records', date_format='iso')
    results = json.loads(j)

    for result in results:
        try:
            insert_sql = "INSERT INTO weather.weather_full (date, tmin, tmax, tavg, prcp, snow, snwd, tanm)\
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)\
                    ON CONFLICT (date)\
                        DO UPDATE SET tmin = %s, tmax = %s, tavg = %s, prcp = %s, snow = %s, snwd = %s, tanm = %s"
            cur.execute(insert_sql, (result['date'][:10], result['TMIN'], result['TMAX'], result['TAVG'], result['PRCP'], result['SNOW'],\
                result['SNWD'], result['TANM'], result['TMIN'], result['TMAX'], result['TAVG'], result['PRCP'], result['SNOW'], result['SNWD'], result['TANM']))
        except:
            print ('could not iterate through results')
    return



# Load weather.weather_{region} tables
def avg_daily_region(region):
    fields = ['TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD']
    dataframes = []
    
    for field in fields:
        query = f"SELECT (wr.date)::date date, AVG((wr.weather_jsonb ->> 'value')::decimal) {field}\
            FROM weather.weather_raw wr\
                WHERE wr.region = '{region}' AND wr.datatype = '{field}'\
                    GROUP BY wr.date\
                        ORDER BY wr.date"
        cur.execute(query)
        results = cur.fetchall()

        flat_results = [result for result in results]
        
        field = pd.DataFrame(flat_results, columns=['date', field])
        dataframes.append(field)
    
    # outer join field DataFrames, add TAVG and TANM
    df = reduce(lambda  left,right: pd.merge(left,right,on=['date'], how='outer'), dataframes)
    df['TAVG'] = df[['TMIN', 'TMAX']].mean(axis=1)
    mean = df[(df['date'] >= date(1951,1,1)) & (df['date'] <= date(1980,12,31))]['TAVG'].mean(axis=0)
    df['TANM'] = df['TAVG'] - mean

    # load results into weather.weather_{region}
    create_table(region)
    j = df.to_json(orient='records', date_format='iso')
    results = json.loads(j)

    for result in results:
        try:
            insert_sql = f"INSERT INTO weather.weather_{region} (date, tmin, tmax, tavg, prcp, snow, snwd, tanm)\
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)\
                    ON CONFLICT (date)\
                        DO UPDATE SET tmin = %s, tmax = %s, tavg = %s, prcp = %s, snow = %s, snwd = %s, tanm = %s"
            cur.execute(insert_sql, (result['date'][:10], result['TMIN'], result['TMAX'], result['TAVG'], result['PRCP'], result['SNOW'],\
                result['SNWD'], result['TANM'], result['TMIN'], result['TMAX'], result['TAVG'], result['PRCP'], result['SNOW'], result['SNWD'], result['TANM']))
        except:
            print ('could not iterate through results')
    return


def load_tables():
    # daily averages of each datatype for all regions
    avg_daily_full()

    # query = "SELECT DISTINCT wr.region FROM weather.weather_raw wr"
    # cur.execute(query)
    # results = cur.fetchall()

    # regions = [result[0] for result in results]
    
    # # daily averages of each datatype for each region (24 regions)
    # for region in regions:
    #     avg_daily_region(region)
    return


load_tables()