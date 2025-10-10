# Docker Postgres Ingestion Pipeline
// filepath: e:\Individual\DE-zoomcamp\1. Docker-sql\README.md
## Tổng quan

Dự án dựng một pipeline đơn giản để tải dữ liệu taxi ở định dạng Parquet từ internet và nạp vào PostgreSQL chạy trong Docker. Toàn bộ môi trường được quản lý bằng `docker-compose`. Ngoài ra còn có pgAdmin để kiểm tra dữ liệu thông qua giao diện web.

## Cấu trúc

- `docker-compose.yml`  
  - `db`: PostgreSQL 14 với thông tin đăng nhập `pipeline_user / pipeline_pass`.  
  - `ingest`: build từ `Dockerfile.ingest`, chạy `ingest_data.py` để tải và nạp dữ liệu.  
  - `pgadmin`: giao diện quản lý PostgreSQL (`http://localhost:5050`, đăng nhập `admin@example.com / admin`).
- `Dockerfile.ingest`: cài đặt Python, wget, pandas, SQLAlchemy, psycopg2-binary và pyarrow, sau đó chạy script ingest.
- `ingest_data.py`: nhận tham số dòng lệnh (URL Parquet, thông tin Postgres, tên bảng) → tải file bằng wget → đọc Parquet → tạo bảng → ghi toàn bộ dữ liệu vào Postgres.
- `test_connection.py`: script kiểm tra (xem bảng tồn tại và đếm số dòng).

## Chuẩn bị

1. Cài Docker Desktop (hoặc Docker Engine).  
2. Đảm bảo thư mục này là workspace hiện tại.  
3. Điều chỉnh `docker-compose.yml` nếu cần:  
   - Thay URL Parquet bằng file bạn muốn (`--url=...`).  
   - Đổi tên bảng (`--table_name=...`).  
   - Khi dùng file lớn, cân nhắc tăng `mem_limit` cho service `ingest`.

## Khởi chạy pipeline

```powershell
docker-compose up -d --build
```

- Docker sẽ dựng Postgres, pgAdmin và container ingest.
- Theo dõi tiến trình ingest (tùy chọn):

```powershell
docker-compose logs -f ingest
```

Sau khi ingest xong, container sẽ in ra thông báo “Finished inserting data”.

## Kiểm tra dữ liệu

### Cách 1: Script `test_connection.py`

```powershell
python test_connection.py
```

Script sẽ:
- Kết nối tới `localhost:5432` (Postgres trong Docker).  
- Kiểm tra bảng `green_taxi_trips`.  
- Đếm số bản ghi và in kết quả.

> Nếu bạn đã đổi `--table_name`, nhớ chỉnh biến `table_name` trong script.

### Cách 2: pgAdmin

1. Mở trình duyệt tới [http://localhost:5050](http://localhost:5050).  
2. Đăng nhập: `admin@example.com / admin`.  
3. Tạo server mới (chuột phải `Servers` → `Create` → `Server...`):
   - **General → Name**: tùy chọn (vd. `docker-postgres`).
   - **Connection**:
     - Host: `postgres_db`
     - Port: `5432`
     - Maintenance DB: `pipeline_db`
     - Username: `pipeline_user`
     - Password: `pipeline_pass`
4. Mở `Databases → pipeline_db → Schemas → public → Tables` để xem bảng `green_taxi_trips`, rồi `View/Edit Data` hoặc `Query Tool` (ví dụ `SELECT COUNT(*) FROM green_taxi_trips;`).

## Dừng và làm sạch

```powershell
docker-compose down
```

Xóa dữ liệu (reset hoàn toàn):

```powershell
docker volume ls        # tìm volume tương ứng, thường là 1_docker-sql_db_data
docker volume rm 1_docker-sql_db_data
```

## Ghi chú

- Khi ingest file nhỏ (ví dụ green taxi 2021-01), `mem_limit: 1g` là đủ. Với file lớn cần tăng lên 2–3 GB hoặc đổi logic đọc theo từng batch.
- Nếu thay đổi URL nhưng muốn giữ dữ liệu cũ, hãy đổi `--table_name`. Nếu muốn ghi đè, giữ tên cũ và script sẽ `replace` schema trước khi ghi.
- Trong trường hợp ingest thất bại, luôn kiểm tra log `docker-compose logs ingest`.