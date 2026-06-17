from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import gc
from dotenv import load_dotenv

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
        try:
            client = storage.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
        except Exception as e:
            print(f"Aviso: Erro ao carregar credenciais do JSON ({e}). Usando credenciais padrão do ambiente.")
            client = storage.Client(project=PROJECT_ID)
    else:
        client = storage.Client(project=PROJECT_ID)
        
    bucket = client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    if blob is None:
        raise ValueError(f"Blob {blob_name} não encontrado no bucket {bucket_name}")
    return blob.updated

def load_raw():
    print("Iniciando carregamento da camada Raw de forma otimizada (baixo uso de RAM)...")
    
    # 1. Busca a data de upload/atualização do arquivo no GCS
    print(f"Buscando metadados do arquivo {BLOB_NAME} no bucket {BUCKET_NAME}...")
    upload_time = get_gcs_file_updated_time(BUCKET_NAME, BLOB_NAME)
    print(f"Data de upload identificada: {upload_time}")
    
    import tempfile
    temp_dir = tempfile.gettempdir()
    local_csv = os.path.join(temp_dir, "temp_raw.csv")
    temp_parquet = os.path.join(temp_dir, "temp_raw.parquet")
    
    # 2. Download do arquivo do GCS para o disco local em stream (evita manter em RAM)
    print(f"Baixando arquivo do GCS para {local_csv}...")
    client_secrets_file = os.getenv("CLIENT_SECRET")
    if client_secrets_file and os.path.exists(client_secrets_file):
        try:
            storage_client = storage.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
        except Exception as e:
            print(f"Aviso: Erro ao carregar credenciais do JSON ({e}). Usando credenciais padrão do ambiente.")
            storage_client = storage.Client(project=PROJECT_ID)
    else:
        storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(BLOB_NAME)
    blob.download_to_filename(local_csv)
    
    if os.path.exists(local_csv):
        print(f"Download concluído! Tamanho do arquivo: {os.path.getsize(local_csv)} bytes")
    else:
        raise FileNotFoundError(f"Erro: O arquivo temporário {local_csv} não foi encontrado após o download!")
    
    # 3. Conversão incremental para Parquet usando chunks
    print("Processando CSV em chunks e gerando Parquet...")
    dtypes = {
        'timestamp': 'int64',
        'sending_address': 'string',
        'receiving_address': 'string',
        'amount': 'string',
        'transaction_type': 'category',
        'location_region': 'category',
        'ip_prefix': 'string',
        'login_frequency': 'string',
        'session_duration': 'string',
        'purchase_pattern': 'category',
        'age_group': 'category',
        'risk_score': 'string',
        'anomaly': 'string'
    }
    
    chunk_size = 50000
    writer = None
    
    for chunk in pd.read_csv(local_csv, dtype=dtypes, na_values=['none', 'None', 'NaN', 'null', ''], chunksize=chunk_size):
        chunk['date_upload_file_bucket'] = pd.Series(pd.to_datetime(upload_time), index=chunk.index).astype('datetime64[us, UTC]')
        
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        
        if writer is None:
            writer = pq.ParquetWriter(temp_parquet, table.schema)
        
        writer.write_table(table)
        
    if writer:
        writer.close()
    
    print("Conversão incremental concluída com sucesso!")
    
    # Remove o CSV temporário para liberar espaço
    if os.path.exists(local_csv):
        os.remove(local_csv)
        
    # 4. Carrega o arquivo Parquet final no BigQuery
    if client_secrets_file and os.path.exists(client_secrets_file):
        try:
            bq_client = bigquery.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
        except Exception as e:
            print(f"Aviso: Erro ao carregar credenciais do JSON ({e}). Usando credenciais padrão do ambiente.")
            bq_client = bigquery.Client(project=PROJECT_ID)
    else:
        bq_client = bigquery.Client(project=PROJECT_ID)
        
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    bq_client.create_dataset(dataset_ref, exists_ok=True)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.PARQUET
    )
    
    print(f"Carregando arquivo Parquet na tabela {table_ref}...")
    with open(temp_parquet, "rb") as source_file:
        job = bq_client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()
        
    # Remove o Parquet temporário
    if os.path.exists(temp_parquet):
        os.remove(temp_parquet)
        
    print(f"Carga da camada Raw concluída com sucesso para {table_ref}!")

if __name__ == '__main__':
    load_raw()
