from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cấu hình cơ bản cho kịch bản
default_args = {
    'owner': 'Thanh Thien',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Khởi tạo DAG
with DAG(
    'ecommerce_daily_batch_pipeline',
    default_args=default_args,
    description='Kịch bản tự động: Tối ưu S3 -> Đẩy Silver vào Postgres -> Chạy dbt Star Schema',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ecommerce', 's3', 'dwh', 'batch'],
) as dag:

    # ==========================================
    # BƯỚC 1: TỐI ƯU HÓA DELTA LAKE TRÊN AWS S3
    # ==========================================
    task_optimize_silver = BashOperator(
        task_id='optimize_silver_layer',
        bash_command='''
            rm -rf /home/airflow/.ivy2/cache && \
            cd /opt/airflow/project && \
            python src/batch/silver_optimize_job.py
        '''
    )

    # ==========================================
    # BƯỚC 2: CHỞ HÀNG TỪ S3 VÀO POSTGRES (DWH)
    # ==========================================
    # Ghi chú: Ghi đè URL kết nối sang mạng nội bộ của Docker (postgres-dwh:5432)
    # vì biến môi trường DWH_HOST trong file .env đang là 'localhost' (dùng cho máy ngoài)
    task_load_to_postgres = BashOperator(
        task_id='load_silver_to_postgres',
        bash_command='''
            cd /opt/airflow/project && \
            export DWH_JDBC_URL="jdbc:postgresql://postgres-dwh:5432/ecommerce_dwh" && \
            export DWH_HOST="postgres-dwh" && \
            export DWH_PORT="5432" && \
            python src/batch/silver_to_dwh.py
        '''
    )

    # ==========================================
    # BƯỚC 3: ĐÚC STAR SCHEMA BẰNG DBT
    # ==========================================
    task_run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='''
            cd /opt/airflow/project/dbt_ecommerce && \
            export DWH_HOST="postgres-dwh" && \
            export DWH_PORT="5432" && \
            /home/airflow/.local/bin/dbt snapshot --profiles-dir . && \
            /home/airflow/.local/bin/dbt run --profiles-dir .
        '''
    )

    # ==========================================
    # QUY ĐỊNH LUỒNG CHẠY (PIPELINE DEPENDENCIES)
    # ==========================================
    # Việc 1 XONG -> Chạy Việc 2 -> Xong -> Chạy Việc 3
    task_optimize_silver >> task_load_to_postgres >> task_run_dbt