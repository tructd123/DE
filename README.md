# Dự án Data Pipeline với Dagster, dbt và Docker

Dự án này xây dựng một pipeline dữ liệu hoàn chỉnh sử dụng các công cụ hiện đại trong lĩnh vực Kỹ thuật Dữ liệu. Pipeline thực hiện việc thu thập dữ liệu (ingest), lưu trữ, biến đổi (transform) và điều phối (orchestrate) một cách tự động và có cấu trúc.

## Công nghệ sử dụng

- **Điều phối (Orchestration)**: [Dagster](https://dagster.io/) - Một công cụ điều phối dữ liệu hiện đại, tập trung vào môi trường phát triển và độ tin cậy của dữ liệu.
- **Biến đổi dữ liệu (Transformation)**: [dbt (Data Build Tool)](https://www.getdbt.com/) - Công cụ cho phép các nhà phân tích và kỹ sư dữ liệu biến đổi dữ liệu trong kho của họ bằng cách viết các câu lệnh SQL có thể kiểm thử và tái sử dụng.
- **Containerization**: [Docker](https://www.docker.com/) & [Docker Compose](https://docs.docker.com/compose/) - Đóng gói toàn bộ ứng dụng và các dịch vụ phụ thuộc vào các container độc lập, đảm bảo tính nhất quán trên các môi trường.
- **Cơ sở dữ liệu**: [PostgreSQL](https://www.postgresql.org/) - Được sử dụng cho cả việc lưu trữ metadata của Dagster và làm kho dữ liệu (data warehouse) cho dữ liệu taxi.
- **Quản lý Database**: [pgAdmin](https://www.pgadmin.org/) - Giao diện web để quản lý và truy vấn database PostgreSQL.

---

## Kiến trúc hệ thống

Hệ thống được xây dựng dựa trên kiến trúc microservices và được quản lý bởi Docker Compose. Mỗi thành phần chạy trong một container riêng biệt, giao tiếp với nhau qua một mạng ảo.

![Kiến trúc hệ thống](httpsd://user-images.githubusercontent.com/10686933/150732923-75075341-f69c-48f6-9188-b0633c3635b3.png)
*(Đây là hình ảnh minh họa kiến trúc tương tự)*

Các services chính bao gồm:

1.  `dagster_webserver`:
    - **Chức năng**: Cung cấp giao diện người dùng (UI) của Dagster (còn gọi là Dagit). Tại đây, bạn có thể xem các pipeline, trạng thái chạy, lịch sử và kích hoạt các tác vụ thủ công.
    - **Port**: `3000`

2.  `dagster_daemon`:
    - **Chức năng**: Service chạy nền, chịu trách nhiệm cho việc lập lịch (schedules) và các cảm biến (sensors), đảm bảo các pipeline được tự động kích hoạt theo lịch trình đã định.

3.  `dagster_user_code`:
    - **Chức năng**: Đây là một thành phần quan trọng trong kiến trúc. Nó chạy mã nguồn pipeline của bạn trong một tiến trình riêng biệt và giao tiếp với các service khác của Dagster thông qua gRPC.
    - **Lợi ích**: Giúp cô lập môi trường của người dùng (thư viện, phiên bản Python) với môi trường hệ thống của Dagster, tăng tính ổn định và cho phép cập nhật code mà không cần khởi động lại toàn bộ hệ thống Dagster.

4.  `postgres`:
    - **Chức năng**: Database PostgreSQL chuyên dụng để lưu trữ metadata cho Dagster, ví dụ như lịch sử chạy, cấu hình pipeline, v.v.

5.  `postgres_zoomcamp`:
    - **Chức năng**: Database PostgreSQL đóng vai trò là kho dữ liệu (Data Warehouse). Đây là nơi dữ liệu taxi thô được tải vào và cũng là nơi dbt thực hiện các quá trình biến đổi.
    - **Port**: `5433` (được map ra bên ngoài để tránh xung đột)

6.  `pgadmin`:
    - **Chức năng**: Cung cấp giao diện web để bạn kết nối, quản lý và truy vấn dữ liệu trong `postgres_zoomcamp`.
    - **Port**: `5050`

---

## Luồng hoạt động của Pipeline

Hệ thống có 2 job chính:

### 1. `postgres_taxi_pipeline` (Ingestion)

- **Mục đích**: Tải dữ liệu taxi (loại `green` và `yellow`) từ một URL public và lưu vào database `postgres_zoomcamp`.
- **Đặc điểm**:
    - **Phân vùng (Partitioned)**: Job này được thiết kế để chạy theo từng tháng. Bạn có thể chọn một tháng cụ thể để tải dữ liệu cho chỉ tháng đó.
    - **Luồng chạy**:
        1.  `download_file_op`: Dựa vào tháng được chọn, op này tạo URL tương ứng và tải file dữ liệu `.csv.gz`.
        2.  `create_table_op`: Tạo bảng trong PostgreSQL nếu chưa tồn tại, với schema đã được định nghĩa sẵn.
        3.  `ingest_data_op`: Đọc file CSV đã tải, xử lý dữ liệu theo từng khối (chunk) và ghi vào bảng `yellow_tripdata` hoặc `green_tripdata` trong database `postgres_zoomcamp`.

### 2. `dbt_transformations_job` (Transformation)

- **Mục đích**: Chạy các model dbt để biến đổi dữ liệu thô (đã được ingest ở bước 1) thành các bảng dữ liệu sạch, có cấu trúc và sẵn sàng cho phân tích.
- **Luồng chạy**:
    1.  Kích hoạt lệnh `dbt build`.
    2.  dbt đọc file `dbt_project.yml` để hiểu về cấu trúc dự án.
    3.  dbt sử dụng `profiles.yml` (được cấu hình qua biến môi trường) để kết nối tới database `postgres_zoomcamp`.
    4.  **Source**: dbt xác định bảng `yellow_tripdata` là nguồn dữ liệu đầu vào.
    5.  **Staging Model**: Chạy model `stg_yellow_tripdata.sql` để làm sạch, đổi tên cột, và tạo ra bảng/view `stg_yellow_tripdata`.
    6.  **Analytics Model**: Chạy model `fact_trips.sql` để join bảng staging với dữ liệu về khu vực (`dim_zones`), tạo ra bảng phân tích cuối cùng là `fact_trips`.

---

## Hướng dẫn sử dụng

### Yêu cầu

- Docker
- Docker Compose

### Khởi chạy hệ thống

1.  Mở terminal và điều hướng đến thư mục `Docker/combined`.
2.  Chạy lệnh sau để xây dựng (build) các image và khởi tạo tất cả các container:
    ```bash
    docker-compose up -d --build
    ```
    - `-d`: Chạy ở chế độ nền (detached mode).
    - `--build`: Buộc Docker xây dựng lại image nếu có bất kỳ thay đổi nào trong `Dockerfile` hoặc mã nguồn.

### Truy cập các dịch vụ

- **Giao diện Dagster**: Mở trình duyệt và truy cập `http://localhost:3000`.
- **Giao diện pgAdmin**: Mở trình duyệt và truy cập `http://localhost:5050`.
    - **Đăng nhập**: Sử dụng thông tin bạn đã cấu hình trong `docker-compose.yml` (nếu có) hoặc tạo mới.
    - **Kết nối server**: Tạo một kết nối mới tới database `postgres_zoomcamp` với các thông tin sau:
        - **Host**: `postgres_zoomcamp`
        - **Port**: `5432` (Đây là port nội bộ trong mạng Docker)
        - **Database**: `postgres-zoomcamp`
        - **Username**: `kestra`
        - **Password**: `k3str4`

### Kích hoạt Pipeline

1.  **Tải dữ liệu (Ingestion)**:
    - Vào giao diện Dagster (`http://localhost:3000`).
    - Chọn job `postgres_taxi_pipeline`.
    - Nhấn vào tab "Partitions" và chọn một tháng bạn muốn tải dữ liệu (ví dụ: `2025-01`).
    - Nhấn "Launch backfill" để bắt đầu.

2.  **Biến đổi dữ liệu (Transformation)**:
    - Sau khi job ingest chạy xong, vào lại giao diện Dagster.
    - Chọn job `dbt_transformations_job`.
    - Nhấn "Materialize all" để chạy.

### Dừng hệ thống

Để dừng tất cả các container, chạy lệnh sau trong thư mục `Docker/combined`:

```bash
docker-compose down
```

---

## Cấu trúc thư mục

```
.
├── Docker/
│   └── combined/
│       └── docker-compose.yml  # Định nghĩa và kết nối tất cả các service
├── flows/
│   ├── dbt_project/            # Thư mục chứa dự án dbt
│   │   ├── models/             # Các model SQL cho việc transform
│   │   ├── profiles.yml        # Cấu hình kết nối DB cho dbt (được gitignore)
│   │   └── dbt_project.yml     # File cấu hình chính của dbt
│   ├── Dockerfile              # Định nghĩa image cho code người dùng
│   ├── postgres_taxi.py        # Định nghĩa pipeline ingest dữ liệu
│   ├── dbt_pipeline.py         # Định nghĩa pipeline chạy dbt
│   ├── repository.py           # Nơi tập hợp tất cả các job để Dagster nhận diện
│   └── workspace.yaml          # Cấu hình để Dagster load code từ gRPC server
└── README.md                   # File này
```
