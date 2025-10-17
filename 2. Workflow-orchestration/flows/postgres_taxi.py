import gzip
import os
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import requests
from dagster import (
    Config,
    ConfigurableResource,
    EnvVar,
    OpExecutionContext,
    ResourceParam,
    job,
    op,
    # highlight-start
    MonthlyPartitionsDefinition,  # <<< THÊM VÀO
    # highlight-end
)

# =====================================================================================
# RESOURCE DEFINITION
# =====================================================================================
class PostgresConnectionResource(ConfigurableResource):
    """Simple psycopg2 wrapper that exposes a context-managed connection."""
    host: str
    port: int
    db_name: str
    user: str
    password: str

    @contextmanager
    def get_connection(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.db_name,
            user=self.user,
            password=self.password,
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

# highlight-start
# =====================================================================================
# PARTITIONS DEFINITION - Thêm định nghĩa phân vùng tháng
# =====================================================================================
monthly_partitions = MonthlyPartitionsDefinition(start_date="2019-01-01")
# highlight-end


# highlight-start
# =====================================================================================
# CONFIGURATION - Đơn giản hóa Config, chỉ cần loại taxi
# =====================================================================================
class TaxiConfig(Config):
    """Định nghĩa loại taxi, năm và tháng sẽ được lấy từ partition."""
    taxi: str = "green"
# highlight-end


# =====================================================================================
# OPS (TASKS) - Các hàm Python thực hiện công việc
# =====================================================================================
@op(description="Tải và giải nén dữ liệu taxi từ URL dựa trên partition key")
def extract_taxi_data(context: OpExecutionContext, config: TaxiConfig) -> Path:
    """
    Tải file .csv.gz, giải nén và lưu vào thư mục tạm của Dagster.
    Ngày tháng giờ đây được lấy từ partition context.
    """
    # highlight-start
    # context.partition_key sẽ là một chuỗi như "2025-10-01"
    # Chúng ta chỉ cần lấy phần năm-tháng từ nó.
    partition_date_str = context.partition_key[:7]  # Lấy "YYYY-MM"
    # highlight-end

    filename = f"{config.taxi}_tripdata_{partition_date_str}.csv"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{config.taxi}/{filename}.gz"
    
    local_path = Path(os.environ["DAGSTER_HOME"]) / "storage" / filename
    local_path.parent.mkdir(parents=True, exist_ok=True)

    context.log.info(f"Downloading for partition {partition_date_str} from {url} to {local_path}")

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f_out:
            with gzip.GzipFile(fileobj=r.raw) as gz:
                f_out.write(gz.read())
    
    return local_path


@op(description="Nạp dữ liệu vào Staging table và biến đổi trong PostgreSQL")
def load_and_transform_in_postgres(
    context: OpExecutionContext,
    config: TaxiConfig,
    file_path: Path,
    db: ResourceParam[PostgresConnectionResource],
):
    """
    Thực hiện toàn bộ logic ELT trong PostgreSQL.
    """
    # highlight-start
    filename = file_path.name  # Lấy tên file từ đối tượng Path
    # highlight-end
    table_name = f"public.{config.taxi}_tripdata"
    staging_table_name = f"{table_name}_staging"

    # --- Logic điều kiện để chọn đúng câu lệnh SQL ---
    if config.taxi == "yellow":
        # Cấu trúc cho Yellow Taxi
        pickup_col = "tpep_pickup_datetime"
        dropoff_col = "tpep_dropoff_datetime"
        columns = [
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
            "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID",
            "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax",
            "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
            "congestion_surcharge"
        ]
        create_table_ddl = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                unique_row_id text PRIMARY KEY, filename text, VendorID text, 
                tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, 
                passenger_count integer, trip_distance double precision, RatecodeID text, 
                store_and_fwd_flag text, PULocationID text, DOLocationID text, 
                payment_type integer, fare_amount double precision, extra double precision, 
                mta_tax double precision, tip_amount double precision, tolls_amount double precision, 
                improvement_surcharge double precision, total_amount double precision, 
                congestion_surcharge double precision
            );
        """
    elif config.taxi == "green":
        # Cấu trúc cho Green Taxi
        pickup_col = "lpep_pickup_datetime"
        dropoff_col = "lpep_dropoff_datetime"
        columns = [
            "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
            "store_and_fwd_flag", "RatecodeID", "PULocationID", "DOLocationID",
            "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax",
            "tip_amount", "tolls_amount", "ehail_fee", "improvement_surcharge",
            "total_amount", "payment_type", "trip_type", "congestion_surcharge"
        ]
        create_table_ddl = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                unique_row_id text PRIMARY KEY, filename text, VendorID text, 
                lpep_pickup_datetime timestamp, lpep_dropoff_datetime timestamp, 
                store_and_fwd_flag text, RatecodeID text, PULocationID text, DOLocationID text, 
                passenger_count integer, trip_distance double precision, fare_amount double precision, 
                extra double precision, mta_tax double precision, tip_amount double precision, 
                tolls_amount double precision, ehail_fee double precision, 
                improvement_surcharge double precision, total_amount double precision, 
                payment_type integer, trip_type integer, congestion_surcharge double precision
            );
        """
    else:
        raise ValueError(f"Unsupported taxi type: {config.taxi}")
    
    # Tạo câu lệnh SQL động
    create_staging_table_ddl = create_table_ddl.replace(table_name, staging_table_name).replace(" PRIMARY KEY", "")
    columns_str = ",".join(columns)
    
    update_staging_sql = f"""
        UPDATE {staging_table_name}
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
        SELECT * FROM {staging_table_name}
        ON CONFLICT (unique_row_id) DO NOTHING;
    """
    
    with db.get_connection() as conn:
        with conn.cursor() as cursor:
            context.log.info(f"Creating tables for {config.taxi} taxi...")
            cursor.execute(create_table_ddl)
            cursor.execute(create_staging_table_ddl)

            context.log.info(f"Truncating staging table {staging_table_name}...")
            cursor.execute(f"TRUNCATE TABLE {staging_table_name};")

            context.log.info(f"Copying data from {file_path} to {staging_table_name}...")
            with open(file_path, "r") as f:
                next(f)
                cursor.copy_expert(f"COPY {staging_table_name} ({columns_str}) FROM STDIN WITH CSV", f)
            
            context.log.info("Adding unique_row_id and filename...")
            cursor.execute(update_staging_sql)

            context.log.info("Merging data into main table...")
            cursor.execute(merge_sql)
            context.log.info(f"Merge complete. {cursor.rowcount} rows affected.")

# =====================================================================================
# JOB - Kết nối các op thành một pipeline
# =====================================================================================
@job(
    description="Pipeline tải dữ liệu taxi NYC và nạp vào PostgreSQL, hỗ trợ partitions.",
    # highlight-start
    partitions_def=monthly_partitions,  # <<< GẮN ĐỊNH NGHĨA PARTITIONS VÀO JOB
    # highlight-end
    resource_defs={
        "db": PostgresConnectionResource(
            host=EnvVar("POSTGRES_HOST"),
            port=EnvVar.int("POSTGRES_PORT"),
            db_name=EnvVar("POSTGRES_DB"),
            user=EnvVar("POSTGRES_USER"),
            password=EnvVar("POSTGRES_PASSWORD"),
        )
    },
)
def postgres_taxi_pipeline():
    """Định nghĩa luồng công việc: extract -> load_and_transform."""
    file_path = extract_taxi_data()
    load_and_transform_in_postgres(file_path=file_path)