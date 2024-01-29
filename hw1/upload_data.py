#!/usr/bin/env python
# coding: utf-8

# In[81]:


import pandas as pd
import psycopg2
import sqlite3
from sqlalchemy import create_engine
import os
import argparse


def main(params):
    user=params.user
    url = params.url
    password=params.password
    host = params.host
    port = params.port
    table = params.table
    db = params.db
    filename = params.filename

    

    if url.endswith('.gz'):
        csv_name = f'{filename}.csv.gz'
    else:
        csv_name = f'{filename}.csv'
        

    os.system(f"wget {url} -O {csv_name}")

    if csv_name == 'output.csv.gz':
        os.system(f"gzip -d {csv_name}")
        csv_name = f'{filename}.csv'

    engine=create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")


    #df = pd.read_csv(csv_name)
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.head(0).to_sql(name=table, con=engine, if_exists='replace')

    if 'lpep_pickup_datetime' in df.columns:
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    
    if 'lpep_dropoff_datetime' in df.columns:
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    df.to_sql(name=table, con=engine, if_exists='append')

    import time

    while True:
        
        start_time = time.time()
        
        df = next(df_iter)
        
        if 'lpep_pickup_datetime' in df.columns:
            df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    
        if 'lpep_dropoff_datetime' in df.columns:
            df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

        df.to_sql(name=table, con=engine, if_exists='append')
        
        end_time = time.time()
        
        print(f'Successfully imported {df.shape[0]} rows in {end_time - start_time} seconds')


if __name__=="__main__":

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument("--user", required=True, help="User in pg database")
    parser.add_argument("--url", required=True, help="URL to data to be uploaded")
    parser.add_argument("--password", required=True, help="Passowrd for DB")
    parser.add_argument("--host", required=True, help="DB host")
    parser.add_argument("--port", required=True, help="Port to access DB")
    parser.add_argument("--table", required=True, help="Table name to populate in PG")
    parser.add_argument("--db", required=True, help="Database name")
    parser.add_argument("--filename", required=True, help="Name for downloaded file")

    args = parser.parse_args()
    main(args)

