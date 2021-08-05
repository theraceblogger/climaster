import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
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

fields = ['TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD']
dataframes = []
for field in fields:
    query = f"SELECT (wr.date)::date, AVG((wr.weather_jsonb ->> 'value')::decimal) {field}\
        FROM weather.weather_raw wr\
            WHERE wr.datatype = '{field}'\
                GROUP BY wr.date\
                    ORDER BY wr.date"
    cur.execute(query)
    results = cur.fetchall()
    # print(results[:5])
    flat_results = []
    for result in results:
        flat_results.append([result[0], result[1]])
    print(flat_results[:5])
    # field = pd.DataFrame(flat_results)
    # print(field.head())
#     dataframes.append(field)
# for x in dataframes:
#     print(x.head())

# df = reduce(lambda  left,right: pd.merge(left,right,on=['date'], how='outer'), dataframes)
# df['tavg'] = df[['tmin', 'tmax']].mean(axis=1)
# print(df.head(10))
# j = df.to_json(orient='records')
# results = json.loads(j)

# for result in results:
#     try:
#         insert_sql = "INSERT INTO weather.weather_daily (date, tmin, tmax, tavg, prcp, snow, snwd)\
#             VALUES (%s,%s,%s,%s,%s,%s,%s)\
#                 ON CONFLICT (date)\
#                     DO UPDATE SET tmin = %s, tmax = %s, tavg = %s, prcp = %s, snow = %s, snwd = %s"
#         cur.execute(insert_sql, (result['date'], result['tmin'], result['tmax'], result['tavg'], result['prcp'], result['snow'],\
#             result['snwd'], result['tmin'], result['tmax'], result['tavg'], result['prcp'], result['snow'], result['snwd']))
#     except:
#         print ('could not iterate through results')