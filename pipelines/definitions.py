"""
Dagster Definitions

Entry point chính cho Dagster project.
Sử dụng Definitions API (Dagster 1.0+).
"""

import os
from pathlib import Path

from dagster import Definitions, EnvVar, define_asset_job, AssetSelection
from dagster_dbt import DbtCliResource

from assets.taxi_ingestion import raw_taxi_file, taxi_tripdata
from assets.export import fact_trips_parquet
from resources.postgres import PostgresResource
from jobs.schedules import (
    yellow_taxi_schedule,
    green_taxi_schedule,
    export_weekly_schedule,
)


# =============================================================================
# RESOURCES
# =============================================================================
DBT_PROJECT_DIR = Path(__file__).resolve().parent / "dbt_project"

resources = {
    "postgres": PostgresResource(
        host=EnvVar("POSTGRES_HOST"),
        port=EnvVar.int("POSTGRES_PORT"),
        database=EnvVar("POSTGRES_DB"),
        user=EnvVar("POSTGRES_USER"),
        password=EnvVar("POSTGRES_PASSWORD"),
    ),
    "dbt": DbtCliResource(
        project_dir=os.fspath(DBT_PROJECT_DIR),
        profiles_dir=os.fspath(DBT_PROJECT_DIR),
        target=os.getenv("DBT_TARGET", "dev"),
    ),
}


# =============================================================================
# JOBS (với config để chạy tuần tự, tránh race condition)
# =============================================================================

# Job cho Yellow Taxi (config sẵn)
yellow_taxi_ingestion_job = define_asset_job(
    name="yellow_taxi_ingestion_job",
    selection=AssetSelection.groups("ingestion"),
    description="Tải và nạp dữ liệu Yellow Taxi vào PostgreSQL",
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 1
                }
            }
        },
        "ops": {
            "raw_taxi_file": {"config": {"taxi_type": "yellow"}},
            "taxi_tripdata": {"config": {"taxi_type": "yellow"}},
        }
    },
)

# Job cho Green Taxi (config sẵn)
green_taxi_ingestion_job = define_asset_job(
    name="green_taxi_ingestion_job",
    selection=AssetSelection.groups("ingestion"),
    description="Tải và nạp dữ liệu Green Taxi vào PostgreSQL",
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 1
                }
            }
        },
        "ops": {
            "raw_taxi_file": {"config": {"taxi_type": "green"}},
            "taxi_tripdata": {"config": {"taxi_type": "green"}},
        }
    },
)

# Job tuỳ chỉnh (Generic) - cho phép user tự chỉnh config trong Launchpad
custom_taxi_ingestion_job = define_asset_job(
    name="custom_taxi_ingestion_job",
    selection=AssetSelection.groups("ingestion"),
    description="Tải và nạp dữ liệu taxi (User tự chọn config yellow/green trong Launchpad)",
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 1  # Chỉ chạy 1 step tại một thời điểm
                }
            }
        }
    },
)

# Job để export sang data lake
export_job = define_asset_job(
    name="export_job",
    selection=AssetSelection.groups("export"),
    description="Export dữ liệu sang Parquet",
)

# Job để chạy dbt transformations
dbt_transformation_job = define_asset_job(
    name="dbt_transformation_job",
    selection=AssetSelection.groups("staging", "core"),
    description="Chạy dbt models để biến đổi dữ liệu",
)


# =============================================================================
# DEFINITIONS
# =============================================================================
defs = Definitions(
    assets=[
        # Taxi ingestion assets
        raw_taxi_file,
        taxi_tripdata,
        # Export assets
        fact_trips_parquet,
    ],
    jobs=[
        yellow_taxi_ingestion_job,
        green_taxi_ingestion_job,
        custom_taxi_ingestion_job,
        export_job,
        dbt_transformation_job,
    ],
    schedules=[
        yellow_taxi_schedule,
        green_taxi_schedule,
        export_weekly_schedule,
    ],
    resources=resources,
)
