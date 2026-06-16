from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import os
import gc
from dotenv import load_dotenv
from extract import extract_data

# Load environment variables
load_dotenv()

PROJECT_ID = "etl-teste-tecnico"
DATASET_ID = "localiza_raw"
TABLE_ID = "raw_fraud_credit"
BUCKET_NAME = "landing-raw"
BLOB_NAME = "df_fraud_credit.csv"

def get_gcs_file_updated_time(bucket_name, blob_name):
    client_secrets_file = os.getenv("CLIENT_SECRET")
    if client_secrets_file and os.path.exists(client_secrets_file):
        client = storage.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
    else:
        client = storage.Client(project=PROJECT_ID)
        
    bucket = client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    if blob is None:
        raise ValueError(f"Blob {blob_name} não encontrado no bucket {bucket_name}")
    return blob.updated

def load_raw():
    print("Iniciando carregamento da camada Raw...")
    
    # Busca a data de upload/atualização do arquivo no GCS
    print(f"Buscando metadados do arquivo {BLOB_NAME} no bucket {BUCKET_NAME}...")
    upload_time = get_gcs_file_updated_time(BUCKET_NAME, BLOB_NAME)
    print(f"Data de upload identificada: {upload_time}")
    
    # Extrai os dados do GCS usando Pandas otimizado
    df = extract_data()
    
    # Adiciona a coluna com a data de upload no bucket
    # Usamos o timestamp em formato UTC e garantimos a conversão correta
    df['date_upload_file_bucket'] = pd.to_datetime(upload_time)
    
    # Salva o DataFrame temporariamente no disco para evitar OOM
    temp_file = "/tmp/temp_raw.parquet"
    os.makedirs(os.path.dirname(temp_file), exist_ok=True)
    
    print(f"Salvando DataFrame Raw temporariamente em {temp_file}...")
    df.to_parquet(temp_file, index=False)
    
    # Limpa DataFrame e força GC
    del df
    gc.collect()
    
    # Carrega o Parquet no BigQuery
    client_secrets_file = os.getenv("CLIENT_SECRET")
    if client_secrets_file and os.path.exists(client_secrets_file):
        client = bigquery.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)
        
    # Garante que o dataset 'raw' existe no BigQuery
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.PARQUET
    )
    
    print(f"Carregando arquivo Parquet na tabela {table_ref}...")
    with open(temp_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()
        
    # Limpa arquivo temporário
    if os.path.exists(temp_file):
        os.remove(temp_file)
        
    print(f"Carga da camada Raw concluída com sucesso para {table_ref}!")

if __name__ == '__main__':
    load_raw()
