from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Adiciona a pasta das DAGs e a subpasta 'src' ao sys.path do Airflow
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

# Importa a função principal do seu pipeline
from main import run_pipeline

default_args = {
    'owner': 'localiza',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'localiza_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL de Fraude de Crédito (GCS -> BigQuery)',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    run_etl_task = PythonOperator(
        task_id='run_etl_pipeline',
        python_callable=run_pipeline,
    )
