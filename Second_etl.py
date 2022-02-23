import datetime
import pandas as pd
import os
import psycopg2
from sys import argv
from dotenv import load_dotenv

name, file_name  = argv

path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(path):
    load_dotenv(path)
PG_LOGIN = os.getenv('PG_LOGIN')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_NAME_BD = os.getenv('PG_NAME_BD')
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')

connection = psycopg2.connect(user=PG_LOGIN,
                                  password=PG_PASSWORD,
                                  host=PG_HOST,
                                  port=PG_PORT,
                                  database=PG_NAME_BD)

cursor = connection.cursor()

def get_cyclones_history ():
    query = """select ch.status, ch.date_from 
                  from cyclones_history ch
                  order by ch.date_from desc;"""
    cursor.execute(query=query)
    return cursor.fetchall()

def insert_into_cyclones_history(date_from, id, status):
    query = f"""insert into cyclones_history (date_from, id, status) values (cast ('{date_from}' as TIMESTAMP),'{id}','{status}');"""
    cursor.execute(query)

dfpred = pd.DataFrame(get_cyclones_history()
                      ,columns=["status","date_from"])
df = pd.read_csv(file_name)

if not dfpred.empty:
    #Если изменилось значение за текущий день
    if dfpred.iloc[0].status != df.iloc[0].status and str(dfpred.iloc[0].date_from) == str(df.iloc[0].date):
        query = f"""delete from cyclones_history
                    where date_from = cast ('{df.iloc[0].date}' as TIMESTAMP);"""

        cursor.execute(query)
    #Если день и статус не отличаются - выходим
    elif dfpred.iloc[0].status == df.iloc[0].status and str(dfpred.iloc[0].date_from) == str(df.iloc[0].date):
        exit(0)
    pred_date = datetime.datetime.strptime(str (df.iloc[0].date), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(days=1)

    # #Если последний статус в таблице не равен новому статусу, и отсутствует дата предыдущего дня, предыдущую запись закрываем
    if dfpred.iloc[0].status != df.iloc[0].status or pred_date != dfpred.iloc[0].date_from:
        query = f"""update cyclones_history 
                       set date_end = cast ('{pred_date}' as timestamp)
                       where date_end is null;"""
        cursor.execute(query)

    insert_into_cyclones_history(df.iloc[0].date,str(df.iloc[0].id),str(df.iloc[0].status))

    #Обновляем фрейм
    dfpred = pd.DataFrame(get_cyclones_history()
                          , columns=["status", "date_from"])

    for i in range(len(dfpred.index)):
        if dfpred.iloc[i].status == df.iloc[0].status and dfpred.iloc[i].date_from == datetime.datetime.strptime(str (dfpred.iloc[i+1].date_from), '%Y-%m-%d %H:%M:%S') + datetime.timedelta(days=1):
            query = f"""update cyclones_history
                        set date_end = null
                        where status = '{dfpred.iloc[i].status}'
                          and date_from = cast ('{dfpred.iloc[i].date_from}' as timestamp);"""
            cursor.execute(query)
        else:
            break
else:
    insert_into_cyclones_history(df.iloc[0].date,str(df.iloc[0].id),str(df.iloc[0].status))

connection.commit()


