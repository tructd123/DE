"""
Schedules

Định nghĩa các schedules cho pipelines.
"""

from dagster import (
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
)


# =============================================================================
# JOB REFERENCE (chỉ dùng cho schedules, không export ra ngoài)
# =============================================================================
# Note: Jobs chính thức được định nghĩa trong definitions.py
# Ở đây chỉ tạo reference cho schedules

_taxi_ingestion_job_for_schedule = define_asset_job(
    name="taxi_ingestion_scheduled",
    selection=AssetSelection.groups("ingestion"),
    description="Scheduled job for taxi ingestion",
)

_export_job_for_schedule = define_asset_job(
    name="export_scheduled",
    selection=AssetSelection.groups("export"),
    description="Scheduled job for export",
)


# =============================================================================
# SCHEDULES
# =============================================================================

# Yellow taxi - ngày 1 hàng tháng lúc 10:00 AM
yellow_taxi_schedule = ScheduleDefinition(
    name="yellow_taxi_monthly_schedule",
    job=_taxi_ingestion_job_for_schedule,
    cron_schedule="0 10 1 * *",
    execution_timezone="Asia/Ho_Chi_Minh",
    run_config={
        "ops": {
            "raw_taxi_file": {"config": {"taxi_type": "yellow"}},
            "taxi_tripdata": {"config": {"taxi_type": "yellow"}},
        }
    },
)

# Green taxi - ngày 1 hàng tháng lúc 9:00 AM  
green_taxi_schedule = ScheduleDefinition(
    name="green_taxi_monthly_schedule",
    job=_taxi_ingestion_job_for_schedule,
    cron_schedule="0 9 1 * *",
    execution_timezone="Asia/Ho_Chi_Minh",
    run_config={
        "ops": {
            "raw_taxi_file": {"config": {"taxi_type": "green"}},
            "taxi_tripdata": {"config": {"taxi_type": "green"}},
        }
    },
)

# Export - hàng tuần vào Chủ Nhật lúc 3:00 AM
export_weekly_schedule = ScheduleDefinition(
    name="export_weekly_schedule",
    job=_export_job_for_schedule,
    cron_schedule="0 3 * * 0",
    execution_timezone="Asia/Ho_Chi_Minh",
)
