## This script takes the data in weather_raw, averages each datatype per day,
## adds a daily average and stores this in weather_clean
import os
import psycopg2
from psycopg2.extras import DictCursor
import json
import pandas as pd
from functools import reduce


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


# get averages for each field, per day, and store in DataFrame
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

    flat_results = []
    for result in results:
        flat_results.append(result)
    
    field = pd.DataFrame(flat_results, columns=['date', field])
    dataframes.append(field)


# outer join field DataFrames, add TAVG
df = reduce(lambda  left,right: pd.merge(left,right,on=['date'], how='outer'), dataframes)
df['TAVG'] = df[['TMIN', 'TMAX']].mean(axis=1)


# load results into weather_clean
j = df.to_json(orient='records', date_format='iso')
results = json.loads(j)

for result in results:
    try:
        insert_sql = "INSERT INTO weather.weather_clean (date, tmin, tmax, tavg, prcp, snow, snwd)\
            VALUES (%s,%s,%s,%s,%s,%s,%s)\
                ON CONFLICT (date)\
                    DO UPDATE SET tmin = %s, tmax = %s, tavg = %s, prcp = %s, snow = %s, snwd = %s"
        cur.execute(insert_sql, (result['date'][:10], result['TMIN'], result['TMAX'], result['TAVG'], result['PRCP'], result['SNOW'],\
            result['SNWD'], result['TMIN'], result['TMAX'], result['TAVG'], result['PRCP'], result['SNOW'], result['SNWD']))
    except:
        print ('could not iterate through results')