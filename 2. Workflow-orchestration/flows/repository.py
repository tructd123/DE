from dagster import repository, schedule, RunRequest, ScheduleEvaluationContext

# Import các job từ các file tương ứng
from getting_started_data_pipeline import getting_started_data_pipeline
from postgres_taxi import postgres_taxi_pipeline
from dbt_pipeline import dbt_pipeline

# =============================================================================
# LỊCH TRÌNH (SCHEDULES)
# =============================================================================

# Tương đương với trigger `yellow_schedule` của Kestra
@schedule(
    job=postgres_taxi_pipeline,
    cron_schedule="0 10 1 * *",  # Chạy lúc 10:00 AM ngày 1 hàng tháng
    execution_timezone="Asia/Ho_Chi_Minh",
)
def yellow_taxi_monthly_schedule(context: ScheduleEvaluationContext):
    """
    Lịch trình này chạy hàng tháng để tải dữ liệu yellow taxi.
    Nó tự động xác định năm và tháng dựa trên ngày chạy.
    """
    # context.scheduled_execution_time tương đương với trigger.date của Kestra
    dt = context.scheduled_execution_time
    
    # Định dạng lại ngày tháng để lấy năm và tháng
    run_key = f"yellow_{dt.strftime('%Y_%m')}"
    year = dt.strftime("%Y")
    month = dt.strftime("%m")

    # Tạo run_config động để truyền vào job
    run_config = {
        "ops": {
            "extract_taxi_data": {"config": {"taxi": "yellow", "year": year, "month": month}},
            "load_and_transform_in_postgres": {"config": {"taxi": "yellow", "year": year, "month": month}},
        }
    }
    
    return RunRequest(run_key=run_key, run_config=run_config)


# Tương đương với trigger `green_schedule` của Kestra
@schedule(
    job=postgres_taxi_pipeline,
    cron_schedule="0 9 1 * *",  # Chạy lúc 9:00 AM ngày 1 hàng tháng
    execution_timezone="Asia/Ho_Chi_Minh",
)
def green_taxi_monthly_schedule(context: ScheduleEvaluationContext):
    """
    Lịch trình này chạy hàng tháng để tải dữ liệu green taxi.
    """
    dt = context.scheduled_execution_time
    
    run_key = f"green_{dt.strftime('%Y_%m')}"
    year = dt.strftime("%Y")
    month = dt.strftime("%m")

    run_config = {
        "ops": {
            "extract_taxi_data": {"config": {"taxi": "green", "year": year, "month": month}},
            "load_and_transform_in_postgres": {"config": {"taxi": "green", "year": year, "month": month}},
        }
    }
    
    return RunRequest(run_key=run_key, run_config=run_config)


# =============================================================================
# REPOSITORY
# =============================================================================
@repository
def zoomcamp_repository():
    """
    Repository chứa tất cả các job VÀ các schedule.
    """
    return [
        getting_started_data_pipeline, 
        postgres_taxi_pipeline,
        # Thêm 2 schedule vào đây để Dagster nhận diện
        yellow_taxi_monthly_schedule,
        green_taxi_monthly_schedule,
        dbt_pipeline,
    ]