# dbt_pipeline.py

import os
from pathlib import Path

from dagster import Config, OpExecutionContext, job, op
from dagster_dbt import DbtCliResource

# Trỏ đến thư mục dự án dbt mà bạn đã copy vào ở bước chuẩn bị
dbt_project_dir = Path(__file__).resolve().parent / "dbt_project"

# =============================================================================
# CONFIGURATION - Tương đương mục `inputs` của Kestra
# =============================================================================
class DbtConfig(Config):
    # Cho phép người dùng chọn lệnh, mặc định là "build"
    dbt_command: str = "build"

# =============================================================================
# OP (TASK) - Tác vụ thực thi dbt
# =============================================================================
@op(
    description="Thực thi các lệnh dbt được chỉ định.",
    required_resource_keys={"dbt"},
)
def dbt_cli_op(context: OpExecutionContext, config: DbtConfig):
    """
    Op này sẽ chạy `dbt deps` sau đó là lệnh được cung cấp trong config.
    """
    # Lấy dbt resource đã được cấu hình trong job
    dbt: DbtCliResource = context.resources.dbt

    # 1. Chạy `dbt deps` để cài đặt các package cần thiết
    context.log.info("Running `dbt deps`...")
    deps_invocation = dbt.cli(["deps"], context=context)
    deps_invocation.wait()
    context.log.info("`dbt deps` finished.")

    # 2. Chạy lệnh chính từ config (ví dụ: "build", "debug")
    dbt_command_str = config.dbt_command
    context.log.info(f"Running `dbt {dbt_command_str}`...")
    
    # Khởi chạy lệnh chính của dbt và chờ hoàn tất
    run_invocation = dbt.cli([dbt_command_str], context=context)
    run_result = run_invocation.wait()
    # Nếu dbt trả về log dòng cuối, ghi lại để tiện theo dõi
    if run_result and hasattr(run_result, "raw_output"):
        context.log.debug(run_result.raw_output)
    
    context.log.info(f"`dbt {dbt_command_str}` finished.")


# =============================================================================
# JOB - Kết nối các op và cấu hình resource
# =============================================================================
@job(
    name="dbt_transformations_job",
    description="Một job để chạy các phép biến đổi dữ liệu bằng dbt.",
    # resource_defs định nghĩa và cấu hình các resource cần thiết cho job
    resource_defs={
        # Đây là nơi cấu hình DbtCliResource, tương đương mục `profiles` của Kestra
        "dbt": DbtCliResource(
            project_dir=os.fspath(dbt_project_dir),
            profiles_dir=os.fspath(dbt_project_dir),
            target=os.getenv("DBT_TARGET", "dev"),
        )
    },
)
def dbt_pipeline():
    """
    Pipeline này chỉ có một bước duy nhất là thực thi dbt.
    """
    dbt_cli_op()