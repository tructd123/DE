"""
Taxi Data Ingestion Assets

Software-Defined Assets cho việc thu thập dữ liệu taxi NYC.
Sử dụng Partitioned Assets để xử lý theo tháng.
"""

import gzip
import os
from pathlib import Path
from typing import Tuple

import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    AssetIn,
    Config,
    DailyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    Output,
    asset,
)

from resources.postgres import PostgresResource


# =============================================================================
# PARTITIONS DEFINITION
# =============================================================================
monthly_partitions = MonthlyPartitionsDefinition(start_date="2019-01-01")


# =============================================================================
# CONFIGURATION
# =============================================================================
class TaxiConfig(Config):
    """Configuration for taxi data ingestion."""
    taxi_type: str = "yellow"  # "yellow" or "green"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def get_taxi_schema(taxi_type: str) -> Tuple[list, str, str]:
    """Get schema configuration based on taxi type."""
    if taxi_type == "yellow":
        columns = [
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
            "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID",
            "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax",
            "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
            "congestion_surcharge"
        ]
        pickup_col = "tpep_pickup_datetime"
        dropoff_col = "tpep_dropoff_datetime"
    else:  # green
        columns = [
            "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
            "store_and_fwd_flag", "RatecodeID", "PULocationID", "DOLocationID",
            "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax",
            "tip_amount", "tolls_amount", "ehail_fee", "improvement_surcharge",
            "total_amount", "payment_type", "trip_type", "congestion_surcharge"
        ]
        pickup_col = "lpep_pickup_datetime"
        dropoff_col = "lpep_dropoff_datetime"
    
    return columns, pickup_col, dropoff_col


def get_create_table_ddl(taxi_type: str, table_name: str) -> str:
    """Generate CREATE TABLE DDL for taxi data."""
    if taxi_type == "yellow":
        return f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                unique_row_id text PRIMARY KEY,
                filename text,
                VendorID text, 
                tpep_pickup_datetime timestamp,
                tpep_dropoff_datetime timestamp, 
                passenger_count integer,
                trip_distance double precision,
                RatecodeID text, 
                store_and_fwd_flag text,
                PULocationID text,
                DOLocationID text, 
                payment_type integer,
                fare_amount double precision,
                extra double precision, 
                mta_tax double precision,
                tip_amount double precision,
                tolls_amount double precision, 
                improvement_surcharge double precision,
                total_amount double precision, 
                congestion_surcharge double precision
            );
        """
    else:  # green
        return f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                unique_row_id text PRIMARY KEY,
                filename text,
                VendorID text, 
                lpep_pickup_datetime timestamp,
                lpep_dropoff_datetime timestamp, 
                store_and_fwd_flag text,
                RatecodeID text,
                PULocationID text,
                DOLocationID text, 
                passenger_count integer,
                trip_distance double precision,
                fare_amount double precision, 
                extra double precision,
                mta_tax double precision,
                tip_amount double precision, 
                tolls_amount double precision,
                ehail_fee double precision, 
                improvement_surcharge double precision,
                total_amount double precision, 
                payment_type integer,
                trip_type integer,
                congestion_surcharge double precision
            );
        """


# =============================================================================
# ASSETS
# =============================================================================
@asset(
    partitions_def=monthly_partitions,
    group_name="ingestion",
    description="Download và lưu file CSV taxi data từ GitHub",
)
def raw_taxi_file(context: AssetExecutionContext, config: TaxiConfig) -> Output[Path]:
    """
    Asset: Download raw taxi data file.
    
    Downloads compressed CSV from GitHub and saves to local storage.
    """
    partition_key = context.partition_key[:7]  # "YYYY-MM"
    taxi_type = config.taxi_type
    
    filename = f"{taxi_type}_tripdata_{partition_key}.csv"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{filename}.gz"
    
    local_path = Path(os.environ.get("DAGSTER_HOME", "/opt/dagster/dagster_home")) / "storage" / filename
    local_path.parent.mkdir(parents=True, exist_ok=True)
    
    context.log.info(f"Downloading {taxi_type} taxi data for {partition_key}")
    context.log.info(f"URL: {url}")
    
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f_out:
            with gzip.GzipFile(fileobj=r.raw) as gz:
                f_out.write(gz.read())
    
    file_size = local_path.stat().st_size / (1024 * 1024)  # MB
    context.log.info(f"Downloaded {file_size:.2f} MB to {local_path}")
    
    return Output(
        local_path,
        metadata={
            "file_path": str(local_path),
            "file_size_mb": file_size,
            "partition": partition_key,
            "taxi_type": taxi_type,
        }
    )


@asset(
    partitions_def=monthly_partitions,
    group_name="ingestion",
    description="Load taxi data vào PostgreSQL với UPSERT",
    deps=[raw_taxi_file],
)
def taxi_tripdata(
    context: AssetExecutionContext,
    config: TaxiConfig,
    postgres: PostgresResource,
) -> Output[int]:
    """
    Asset: Load taxi data into PostgreSQL.
    
    Loads data into staging table, generates unique IDs, then merges.
    """
    partition_key = context.partition_key[:7]
    taxi_type = config.taxi_type
    
    filename = f"{taxi_type}_tripdata_{partition_key}.csv"
    file_path = Path(os.environ.get("DAGSTER_HOME", "/opt/dagster/dagster_home")) / "storage" / filename
    
    table_name = f"public.{taxi_type}_tripdata"
    staging_table = f"{table_name}_staging"
    
    columns, pickup_col, dropoff_col = get_taxi_schema(taxi_type)
    create_table_ddl = get_create_table_ddl(taxi_type, table_name)
    create_staging_ddl = create_table_ddl.replace(table_name, staging_table).replace(" PRIMARY KEY", "")
    
    update_staging_sql = f"""
        UPDATE {staging_table}
        SET 
            unique_row_id = md5(
                COALESCE(CAST(VendorID AS text), '') ||
                COALESCE(CAST({pickup_col} AS text), '') || 
                COALESCE(CAST({dropoff_col} AS text), '') || 
                COALESCE(PULocationID, '') || 
                COALESCE(DOLocationID, '') || 
                COALESCE(CAST(fare_amount AS text), '') || 
                COALESCE(CAST(trip_distance AS text), '')
            ),
            filename = '{filename}';
    """
    
    merge_sql = f"""
        INSERT INTO {table_name}
        SELECT * FROM {staging_table}
        ON CONFLICT (unique_row_id) DO NOTHING;
    """
    
    rows_inserted = 0
    
    with postgres.get_connection() as conn:
        # Bước 1: Tạo bảng với advisory lock (riêng transaction)
        # Sử dụng autocommit để tránh transaction bị abort ảnh hưởng các lệnh sau
        conn.autocommit = True
        with conn.cursor() as cursor:
            # Use advisory lock to prevent race condition when multiple partitions
            # try to create the same table simultaneously
            context.log.info(f"Acquiring lock for table creation...")
            cursor.execute("SELECT pg_advisory_lock(hashtext(%s))", (table_name,))
            
            try:
                context.log.info(f"Creating tables for {taxi_type} taxi...")
                # CREATE TABLE IF NOT EXISTS sẽ không fail nếu table đã tồn tại
                cursor.execute(create_table_ddl)
                cursor.execute(create_staging_ddl)
                context.log.info(f"Tables created/verified successfully")
            except Exception as e:
                # Nếu lỗi không phải do table exists, raise lại
                if "already exists" not in str(e):
                    raise
                context.log.info(f"Tables already exist, continuing...")
            finally:
                cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", (table_name,))
        
        # Bước 2: Load data với transaction thông thường
        conn.autocommit = False
        with conn.cursor() as cursor:
            context.log.info(f"Truncating staging table...")
            cursor.execute(f"TRUNCATE TABLE {staging_table};")
            
            context.log.info(f"Loading data from {file_path}...")
            columns_str = ",".join(columns)
            with open(file_path, "r") as f:
                next(f)  # Skip header
                cursor.copy_expert(f"COPY {staging_table} ({columns_str}) FROM STDIN WITH CSV", f)
            
            context.log.info("Generating unique IDs...")
            cursor.execute(update_staging_sql)
            
            context.log.info("Merging into main table...")
            cursor.execute(merge_sql)
            rows_inserted = cursor.rowcount
            
            conn.commit()
            context.log.info(f"Inserted {rows_inserted} rows")
    
    return Output(
        rows_inserted,
        metadata={
            "rows_inserted": rows_inserted,
            "table": table_name,
            "partition": partition_key,
        }
    )
