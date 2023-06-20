import os

from time import time

import pandas as pd
import pyarrow.parquet as pq

from sqlalchemy import create_engine

def main(table_name, parquet_file, execution_date):
    print('connection established successfully, initializing ingestion for: ')
    print(table_name, csv_file, execution_date)

    t_start = time()

    file_name = parquet_file.replace('.parquet', '')
    #convert to csv to use iterator 
    df = pd.read_parquet(parquet_file)
    df.to_csv(str(file_name))

    df_iter = pd.read_csv(file_name + '.csv', iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    t_end = time()
    print('inserted the first chunk, took %.3f second' % (t_end - t_start))

    while True: 
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print("completed")
            break

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

def connect_pg(user, password, host, port, db):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully')


if __name__ == '__main__':

    main()
