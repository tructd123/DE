"""
Export Assets

Assets cho việc export dữ liệu sang Data Lake (Parquet).
"""

import os
from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, Output, asset
from sqlalchemy import create_engine

from resources.postgres import PostgresResource


@asset(
    group_name="export",
    description="Export bảng fact_trips sang Parquet file",
    deps=["fact_trips"],  # Depends on dbt fact_trips model
)
def fact_trips_parquet(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> Output[Path]:
    """
    Asset: Export fact_trips to Parquet.
    
    Đọc dữ liệu từ PostgreSQL và lưu sang định dạng Parquet.
    """
    context.log.info("Reading fact_trips from PostgreSQL...")
    
    engine = create_engine(postgres.get_connection_string())
    df = pd.read_sql("SELECT * FROM public.fact_trips", engine)
    
    output_path = Path("/opt/dagster/data_lake/fact_trips.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    context.log.info(f"Saving {len(df)} rows to {output_path}...")
    df.to_parquet(output_path, index=False)
    
    file_size = output_path.stat().st_size / (1024 * 1024)  # MB
    
    return Output(
        output_path,
        metadata={
            "row_count": len(df),
            "file_path": str(output_path),
            "file_size_mb": round(file_size, 2),
        }
    )
