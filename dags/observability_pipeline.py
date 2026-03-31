import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv
from airflow.operators.bash import BashOperator

ENV_PATH ="/opt/airflow/project/.env"
load_dotenv(ENV_PATH)

# lấy từ .env
SCHEDULE = os.getenv("OBSERVABILITY_PIPELINE_SCHEDULE")
COMPACTION_SCRIPT = os.getenv("COMPACTION_SCRIPT_PATH")
LOG_AGGREGATOR_SCRIPT = os.getenv("LOG_AGGREGATOR_SCRIPT_PATH")
DAG_OWNER = os.getenv("DAG_OWNER")
OBSERVABILITY_SCHEDULE = os.getenv("OBSERVABILITY_PIPELINE_SCHEDULE")

default_args ={
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'retries':1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'system_observability_pipeline',
    default_args=default_args,
    description='Tự động dọn rác DLQ và gom Log hệ thống (Phụ thuộc 100% vào .env)',
    schedule_interval=SCHEDULE, 
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['monitoring', 'maintenance'],
) as dag:
    task_dlq_compaction = BashOperator(
        task_id='run_dlq_compaction',
        bash_command=f'''
            cd /opt/airflow/project && \
            python {COMPACTION_SCRIPT}
        '''
    )

    task_log_aggregator = BashOperator(
        task_id='run_log_aggregator',
        # Thêm lệnh pip install polars python-dotenv vào trước lệnh python
        bash_command=f'pip install polars python-dotenv && cd /opt/airflow/project && python {LOG_AGGREGATOR_SCRIPT}'
    )

    task_dlq_compaction >> task_log_aggregator