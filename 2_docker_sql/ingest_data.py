#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from time import time
import os

import argparse

def main(params):
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table = params.table
    url = params.url

    csv_name = "output.csv.gz"

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    df_iter = pd.read_csv(csv_name, iterator = True, chunksize=100000, compression='gzip')
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table, con=engine, if_exists='replace') 

    df.to_sql(name=table, con=engine, if_exists='append')



    while True:
        t_start = time()
        
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table, con=engine, if_exists='append')
        
        t_end = time()
        
        print('inserted another chunk, took %.3f second(s)' %(t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest csv to Postgres')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--database', help='database name for postgres')
    parser.add_argument('--table', help='name of the table for postgres')
    parser.add_argument('--url', help='url of the csv')

    args = parser.parse_args()
    main(args)
