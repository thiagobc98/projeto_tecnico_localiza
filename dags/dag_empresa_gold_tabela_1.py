from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Adiciona a pasta das DAGs e a subpasta 'src' ao sys.path do Airflow
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from localiza_gold import load_gold_tabela_1

default_args = {
    'owner': 'localiza',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'localiza_gold_tabela_1_pipeline',
    default_args=default_args,
    description='Pipeline da Camada Gold - Tabela 1 (region_risk_average)',
    schedule_interval=None, # Execução manual
    catchup=False,
) as dag:

    task_gold_tabela_1 = PythonOperator(
        task_id='localiza_gold_tabela_1',
        python_callable=load_gold_tabela_1,
    )
