# Dự án Data Pipeline với Dagster, dbt và Docker

Dự án này xây dựng một pipeline dữ liệu hoàn chỉnh sử dụng các công cụ hiện đại trong lĩnh vực Kỹ thuật Dữ liệu. Pipeline thực hiện việc thu thập dữ liệu (ingest), lưu trữ, biến đổi (transform) và điều phối (orchestrate) một cách tự động và có cấu trúc.

## Công nghệ sử dụng

- **Điều phối (Orchestration)**: [Dagster](https://dagster.io/) - Công cụ điều phối dữ liệu hiện đại với Software-Defined Assets.
- **Biến đổi dữ liệu (Transformation)**: [dbt](https://www.getdbt.com/) - Transform dữ liệu bằng SQL.
- **Containerization**: [Docker](https://www.docker.com/) & Docker Compose.
- **Cơ sở dữ liệu**: [PostgreSQL](https://www.postgresql.org/) - Data Warehouse.
- **Quản lý Database**: [pgAdmin](https://www.pgadmin.org/).

---

## Kiến trúc hệ thống

```
┌────────────────────────────────────────────────────────────────────┐
│                         DOCKER COMPOSE                              │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   │
│  │  dagster_       │   │  dagster_       │   │  dagster_       │   │
│  │  webserver      │◄──│  user_code      │◄──│  daemon         │   │
│  │  (UI:3000)      │   │  (gRPC:4000)    │   │  (Scheduler)    │   │
│  └────────┬────────┘   └────────┬────────┘   └────────┬────────┘   │
│           │                     │                     │            │
│           ▼                     ▼                     ▼            │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   │
│  │   postgres      │   │ postgres_       │   │   pgadmin       │   │
│  │   (Metadata)    │   │ zoomcamp        │   │   (UI:5050)     │   │
│  │                 │   │ (Data:5433)     │   │                 │   │
│  └─────────────────┘   └─────────────────┘   └─────────────────┘   │
└────────────────────────────────────────────────────────────────────┘
```

---

## Cấu trúc thư mục

```
DE-zoomcamp/
├── Docker/
│   └── dagster/
│       └── docker-compose.yml      # Định nghĩa services
├── pipelines/                      # Dagster pipelines
│   ├── __init__.py
│   ├── definitions.py              # Entry point (Definitions API)
│   ├── workspace.yaml              # Dagster workspace config
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── resources/                  # Database resources
│   │   ├── __init__.py
│   │   └── postgres.py
│   ├── assets/                     # Software-Defined Assets
│   │   ├── __init__.py
│   │   ├── taxi_ingestion.py       # Taxi data ingestion
│   │   ├── dbt_models.py           # dbt integration
│   │   └── export.py               # Export to Parquet
│   ├── jobs/                       # Jobs & Schedules
│   │   ├── __init__.py
│   │   └── schedules.py
│   └── dbt_project/                # dbt project
│       ├── models/
│       │   ├── staging/
│       │   └── core/
│       └── dbt_project.yml
├── data_lake/                      # Output Parquet files
└── README.md
```

---

## Assets

| Asset | Group | Description |
|-------|-------|-------------|
| `raw_taxi_file` | ingestion | Download taxi CSV từ GitHub |
| `taxi_tripdata` | ingestion | Load vào PostgreSQL |
| `fact_trips_parquet` | export | Export sang Parquet |

---

## Hướng dẫn sử dụng

### Khởi chạy hệ thống

```bash
cd Docker/dagster
docker-compose up -d --build
```

### Truy cập

- **Dagster UI**: http://localhost:3000
- **pgAdmin**: http://localhost:5050
  - Email: `admin@example.com`
  - Password: `admin`

### Materialize Assets

1. Mở Dagster UI
2. Vào tab **Assets**
3. Chọn assets cần materialize
4. Click **Materialize**

### Chạy với Partition (tháng cụ thể)

1. Chọn asset `raw_taxi_file` hoặc `taxi_tripdata`
2. Click **Materialize**
3. Chọn partition (ví dụ: `2019-01-01`)
4. Cấu hình `taxi_type`: `"yellow"` hoặc `"green"`

### Dừng hệ thống

```bash
docker-compose down
```

---

## Schedules

| Schedule | Cron | Mô tả |
|----------|------|-------|
| `yellow_taxi_monthly_schedule` | `0 10 1 * *` | Tải Yellow Taxi ngày 1 hàng tháng |
| `green_taxi_monthly_schedule` | `0 9 1 * *` | Tải Green Taxi ngày 1 hàng tháng |
| `export_weekly_schedule` | `0 3 * * 0` | Export Parquet hàng tuần |
