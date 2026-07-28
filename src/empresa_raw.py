from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import gc
from dotenv import load_dotenv

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
    
    # Configura os clientes
    client_secrets_file = os.getenv("CLIENT_SECRET")
    if client_secrets_file and os.path.exists(client_secrets_file):
        try:
            bq_client = bigquery.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
        except Exception as e:
            print(f"Aviso: Erro ao carregar credenciais do JSON ({e}). Usando credenciais padrão.")
            bq_client = bigquery.Client(project=PROJECT_ID)
    else:
        bq_client = bigquery.Client(project=PROJECT_ID)

    if client_secrets_file and os.path.exists(client_secrets_file):
        try:
            storage_client = storage.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
        except Exception as e:
            print(f"Aviso: Erro ao carregar credenciais do JSON ({e}). Usando credenciais padrão.")
            storage_client = storage.Client(project=PROJECT_ID)
    else:
        storage_client = storage.Client(project=PROJECT_ID)

    # Obtém o maior timestamp de arquivo já processado no BigQuery
    max_raw_query = f"SELECT MAX(date_upload_file_bucket) as max_raw FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
    try:
        raw_result = list(bq_client.query(max_raw_query).result())
        max_raw_bq = raw_result[0]['max_raw'] if raw_result else None
    except Exception as e:
        print(f"Tabela RAW não encontrada ou vazia (primeira execução): {e}")
        max_raw_bq = None

    if max_raw_bq:
        max_raw_bq = pd.to_datetime(max_raw_bq, utc=True).to_pydatetime()
        print(f"Maior data de upload já registrada no BigQuery: {max_raw_bq}")
    else:
        print("Nenhuma data de upload registrada no BigQuery (tabela vazia ou inexistente).")

    # Lista arquivos .csv no bucket GCS
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs()
    csv_blobs = [b for b in blobs if b.name.lower().endswith('.csv')]
    
    # Filtra e ordena arquivos novos por ordem cronológica (mais antigo primeiro)
    new_blobs = []
    for b in csv_blobs:
        blob_updated = pd.to_datetime(b.updated, utc=True).to_pydatetime()
        # Se for mais recente que max_raw_bq (ou se max_raw_bq for None)
        if max_raw_bq is None or blob_updated > max_raw_bq:
            new_blobs.append((b, blob_updated))
            
    # Ordena pelo timestamp de atualização
    new_blobs.sort(key=lambda x: x[1])

    if not new_blobs:
        print("Nenhum arquivo novo detectado no bucket. Finalizando etapa.")
        return

    # Pega o arquivo novo mais antigo da fila para processar nesta rodada
    target_blob, upload_time = new_blobs[0]
    print(f"Arquivo selecionado para processamento: {target_blob.name} (Upload: {upload_time})")

    import tempfile
    temp_dir = tempfile.gettempdir()
    local_csv = os.path.join(temp_dir, "temp_raw.csv")
    temp_parquet = os.path.join(temp_dir, "temp_raw.parquet")
    
    # Download do arquivo selecionado do GCS para o disco local em stream
    print(f"Baixando arquivo {target_blob.name} do GCS para {local_csv}...")
    target_blob.download_to_filename(local_csv)
    
    if os.path.exists(local_csv):
        print(f"Download concluído! Tamanho do arquivo: {os.path.getsize(local_csv)} bytes")
    else:
        raise FileNotFoundError(f"Erro: O arquivo temporário {local_csv} não foi encontrado após o download!")
    
    # Conversão incremental para Parquet usando chunks
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
        
    # Carrega o arquivo Parquet final no BigQuery
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
