import pandas as pd
from dagster import op, job, get_dagster_logger
from sqlalchemy import create_engine

# (Giả sử bạn dùng lại Resource Posretgs từ job khác,
# hoặc bạn có thể kết nối trực tiếp như ví dụ này)

@op
def export_fact_trips_to_parquet():
    """
    Đọc bảng fact_trips từ Postgres và lưu dưới dạng file Parquet
    vào thư mục data_lake đã được mount.
    """
    log = get_dagster_logger()
    
    # Kết nối vào Postgres (chạy trong Docker)
    db_uri = "postgresql://kestra:k3str4@postgres_zoomcamp:5432/postgres-zoomcamp"
    engine = create_engine(db_uri)
    
    log.info("Reading data from postgres_zoomcamp...")
    df = pd.read_sql("SELECT * FROM public.fact_trips", engine)

    # Đường dẫn lưu file bên trong container
    output_path = "/opt/dagster/data_lake/fact_trips.parquet"
    
    log.info(f"Saving data to {output_path}...")
    df.to_parquet(output_path, index=False)
    log.info("Save complete!")

@job
def export_to_local_lake_job():
    export_fact_trips_to_parquet()