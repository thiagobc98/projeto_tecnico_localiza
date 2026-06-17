from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import sys
import os

# Adiciona a pasta das DAGs e a subpasta 'src' ao sys.path do Airflow
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

# Módulos são carregados dinamicamente dentro das funções de execução para evitar cache nos workers

def check_new_file_condition():
    from google.cloud import bigquery
    import pandas as pd
    project_id = "etl-teste-tecnico"
    client = bigquery.Client(project=project_id)
    
    def to_utc_datetime(val):
        if val is None:
            return None
        # Se for numérico 
        if isinstance(val, (int, float)):
            if val > 1e11:  # Nanossegundos
                return pd.to_datetime(val, unit='ns', utc=True).to_pydatetime()
            else:          # Segundos
                return pd.to_datetime(val, unit='s', utc=True).to_pydatetime()
        # Se for string
        if isinstance(val, str):
            return pd.to_datetime(val, utc=True).to_pydatetime()
        # Caso contrário (datetime/Timestamp), normaliza para UTC
        return pd.to_datetime(val, utc=True).to_pydatetime()

    # Obtém a data de upload do arquivo que acabou de ser processado na RAW
    raw_query = f"SELECT MAX(date_upload_file_bucket) as max_raw FROM `{project_id}.localiza_raw.raw_fraud_credit`"
    try:
        raw_result = list(client.query(raw_query).result())
        max_raw = raw_result[0]['max_raw'] if raw_result else None
    except Exception as e:
        print(f"Erro ao buscar data da tabela RAW: {e}")
        max_raw = None
        
    if not max_raw:
        raise AirflowSkipException("Nenhum dado encontrado na tabela RAW. Pulando etapas posteriores.")
        
    # Obtém o maior timestamp de arquivo já processado na Bronze
    bronze_query = f"SELECT MAX(dat_data_upload_bucket) as max_bronze FROM `{project_id}.localiza_bronze.localiza_bronze`"
    try:
        bronze_result = list(client.query(bronze_query).result())
        max_bronze = bronze_result[0]['max_bronze'] if bronze_result else None
    except Exception as e:
        print(f"Tabela Bronze não encontrada ou vazia (primeira execução): {e}")
        max_bronze = None
        
    # Normaliza ambas as datas para comparação segura
    max_raw_dt = to_utc_datetime(max_raw)
    max_bronze_dt = to_utc_datetime(max_bronze)
    
    print(f"Data arquivo RAW (convertida): {max_raw_dt} | Maior data já processada na Bronze (convertida): {max_bronze_dt}")
    
    # Valida se o arquivo é novo
    if max_bronze_dt is None or max_raw_dt > max_bronze_dt:
        print("Novo arquivo detectado no bucket! Prosseguindo para as próximas camadas.")
        return True
    else:
        print("O arquivo no bucket é antigo ou idêntico ao já processado. Pulando Bronze, Silver e Gold.")
        raise AirflowSkipException("Nenhum arquivo novo detectado no bucket.")

def run_raw_pipeline():
    import gc
    import importlib
    import localiza_raw
    importlib.reload(localiza_raw)
    from localiza_raw import load_raw
    
    print("Iniciando processamento da camada Raw...")
    load_raw()
    gc.collect()

def run_bronze_pipeline():
    import gc
    import importlib
    import extract_raw
    import localiza_bronze
    importlib.reload(extract_raw)
    importlib.reload(localiza_bronze)
    from extract_raw import extract_raw
    from localiza_bronze import load_bronze, tratamento_bronze
    
    print("Iniciando processamento da camada Bronze...")
    df_raw = extract_raw()
    df_bronze = tratamento_bronze(df_raw)
    del df_raw
    gc.collect()
    
    load_bronze(df_bronze)
    del df_bronze
    gc.collect()

def run_silver_pipeline():
    import gc
    import importlib
    import extract_bronze
    import localiza_silver
    importlib.reload(extract_bronze)
    importlib.reload(localiza_silver)
    from extract_bronze import extract_bronze
    from localiza_silver import load_silver, transform_silver
    
    print("Iniciando processamento da camada Silver...")
    df_bronze = extract_bronze()
    df_silver = transform_silver(df_bronze)
    del df_bronze
    gc.collect()
    
    load_silver(df_silver)
    del df_silver
    gc.collect()

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
    schedule_interval='0 10 * * *', # Executa todos os dias às 10h da manhã 
    catchup=False,
) as dag:

    task_raw = PythonOperator(
        task_id='localiza_raw',
        python_callable=run_raw_pipeline,
    )

    task_check_condition = PythonOperator(
        task_id='check_new_file_condition',
        python_callable=check_new_file_condition,
    )

    task_bronze = PythonOperator(
        task_id='localiza_bronze',
        python_callable=run_bronze_pipeline,
    )

    task_silver = PythonOperator(
        task_id='localiza_silver',
        python_callable=run_silver_pipeline,
    )

    # Fluxo de execução
    task_raw >> task_check_condition >> task_bronze >> task_silver

