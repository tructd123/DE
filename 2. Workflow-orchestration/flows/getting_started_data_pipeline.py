import requests
import pandas as pd
from dagster import op, job, In, Output, MetadataValue

# Tác vụ 1: Extract - Tương đương task "extract"
# Op này không có đầu vào và trả về một danh sách (list) các sản phẩm.
@op
def extract_products() -> list:
    """Tải dữ liệu sản phẩm từ API và trả về JSON."""
    response = requests.get("https://dummyjson.com/products")
    response.raise_for_status() # Báo lỗi nếu request thất bại
    return response.json()["products"]

# Tác vụ 2: Transform - Tương đương task "transform"
# Op này nhận đầu vào là danh sách sản phẩm từ op trước đó.
# Tham số `columns_to_keep` có thể được cấu hình qua config.
@op(
    config_schema={"columns_to_keep": list},
    ins={"products": In(list)}
)
def transform_products(context, products: list) -> list:
    """Lọc dữ liệu để chỉ giữ lại các cột cần thiết."""
    columns_to_keep = context.op_config["columns_to_keep"]
    
    filtered_data = [
        {column: product.get(column, "N/A") for column in columns_to_keep}
        for product in products
    ]
    return filtered_data

# Tác vụ 3: Query - Tương đương task "query"
# Op này nhận dữ liệu đã lọc và tính toán giá trung bình.
@op(ins={"filtered_products": In(list)})
def aggregate_average_price(filtered_products: list) -> pd.DataFrame:
    """
    Sử dụng Pandas để nhóm theo thương hiệu và tính giá trung bình.
    Trả về một DataFrame của Pandas.
    """
    if not filtered_products:
        return pd.DataFrame(columns=["brand", "avg_price"])

    df = pd.DataFrame(filtered_products)
    
    # Chuyển đổi kiểu dữ liệu an toàn
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df = df.dropna(subset=['price'])
    
    avg_price_df = df.groupby('brand')['price'].mean().round(2).reset_index()
    avg_price_df = avg_price_df.rename(columns={'price': 'avg_price'})
    avg_price_df = avg_price_df.sort_values(by='avg_price', ascending=False)
    
    print("Báo cáo giá trung bình theo thương hiệu:")
    print(avg_price_df)
    return avg_price_df

# Định nghĩa Job: Kết nối các op lại với nhau
@job
def getting_started_data_pipeline():
    """
    Định nghĩa luồng dữ liệu bằng cách gọi các op theo đúng thứ tự
    và truyền dữ liệu giữa chúng.
    """
    raw_products = extract_products()
    filtered_list = transform_products(raw_products)
    aggregate_average_price(filtered_list)