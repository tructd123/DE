#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    print("Starting data ingestion process...")

    if url.endswith('.parquet'):
        csv_name = 'output.parquet'
    else:
        print("URL does not point to a parquet file. Exiting.")
        return

    # Download the file
    print(f"Downloading data from {url}...")
    os.system(f"wget {url} -O {csv_name}")
    print("Download complete.")

    # Create database engine
    print("Creating database engine...")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print("Database engine created.")

    # Read parquet file
    print(f"Reading parquet file: {csv_name}...")
    df = pd.read_parquet(csv_name)
    print("Parquet file read into DataFrame.")

    # Create table schema
    print(f"Creating table schema for '{table_name}'...")
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print("Table schema created successfully.")

    # Insert data into the table
    print(f"Inserting data into table '{table_name}'...")
    start_time = time()
    
    # Use a try-except block to catch potential errors during insertion
    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=100000)
        end_time = time()
        print(f"Finished inserting data. Time taken: {end_time - start_time:.2f} seconds.")
    except Exception as e:
        print(f"An error occurred during data insertion: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the parquet file')

    args = parser.parse_args()

    main(args)