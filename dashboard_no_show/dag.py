from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl_logic import run_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'dashboard_no_show_etl',
    default_args=default_args,
    schedule_interval='0 1 * * *'
)

etl_task = PythonOperator(
    task_id='run_etl_process',
    python_callable=run_etl,
    dag=dag
)