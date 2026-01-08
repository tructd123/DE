"""
dbt Assets

Integration với dbt sử dụng dagster-dbt.
"""

import os
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DbtCliResource, dbt_assets


# =============================================================================
# DBT PROJECT CONFIGURATION
# =============================================================================
DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent / "dbt_project"


@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    select="taxi_rides_ny",
)
def dbt_taxi_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Asset: dbt models cho taxi data transformation.
    
    Bao gồm:
    - Staging models: stg_yellow_tripdata, stg_green_tripdata
    - Core models: fact_trips, dim_zones, dm_monthly_zone_revenue
    """
    yield from dbt.cli(["build"], context=context).stream()


# Pre-configured dbt resource
dbt_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    profiles_dir=os.fspath(DBT_PROJECT_DIR),
    target=os.getenv("DBT_TARGET", "dev"),
)
