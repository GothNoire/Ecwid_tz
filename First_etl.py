import datetime
import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd
from sys import argv


path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(path):
    load_dotenv(path)
PG_LOGIN = os.getenv('PG_LOGIN')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_NAME_BD = os.getenv('PG_NAME_BD')
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')

name, date  = argv


connection = psycopg2.connect(user=PG_LOGIN,
                                  password=PG_PASSWORD,
                                  host=PG_HOST,
                                  port=PG_PORT,
                                  database=PG_NAME_BD)
cursor = connection.cursor()

query = f"""with t as (
            select cast (cast (c."Date" as varchar) as TIMESTAMP) + c."Time" * interval '1 second' format_date
            --,c."Date"
            --,c."Time" 
            --,cast (cast (c."Date" as varchar) as TIMESTAMP)
            ,c."ID" id
            ,c."Status" status
            from cyclones c 
            where date_trunc('month', cast (cast (c."Date" as varchar) as TIMESTAMP)) = date_trunc('month',cast ('{date}' as TIMESTAMP))
            order by format_date
            )
                
            select t1.id, t1.format_date, t1.status from 
            (
            select max(t.format_date) col
            from t
            group by date_trunc('day', t.format_date) 
            )tab, t t1
            where t1.format_date = tab.col;"""

cursor.execute(query=query)
df = pd.DataFrame(cursor.fetchall(),
                columns=['id','date','status'])

for i in range(len(df.index)):
    file_name = str(df.iloc[i]['date'].year) + str(df.iloc[i]['date'].month) + str(df.iloc[i]['date'].day)
    df[i:i+1].to_csv(f'cyclones_{file_name}.csv')
