from google.cloud import bigquery
import pandas as pd
import os
import gc
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = "etl-teste-tecnico"
DATASET_ID = "localiza_silver"
TABLE_ID = "localiza_silver"

def transform_silver(df: pd.DataFrame) -> pd.DataFrame:
    print("Iniciando limpeza e transformações para a camada Silver (nomes em português)...")
    
    # Remove duplicatas baseadas na transação
    linhas_antes = len(df)
    df = df.drop_duplicates(
        subset=['dat_data_transaction', 'cod_endereco_enviado', 'cod_endereco_recebido']
    )
    print(f"Duplicatas removidas: {linhas_antes - len(df)} linhas.")
    
    # Remove nulos nas colunas essenciais
    df = df.dropna(subset=['dat_data_transaction', 'cod_endereco_enviado', 'cod_endereco_recebido', 'vlr_valor'])

    # Filtra transações com valores negativos 
    df = df[df['vlr_valor'] >= 0]
    
    # Substitui valores '0' ou 0 na coluna 'des_regiao' por NA
    df['des_regiao'] = df['des_regiao'].replace(['0', 0], pd.NA)
    
    # Padroniza strings para minúsculo nas colunas
    for col in df.select_dtypes(include=['object', 'category', 'string']).columns:
        df[col] = df[col].astype(str).str.strip().str.lower()
            
    print(f"Transformações concluídas! Total de linhas para Silver: {len(df)}")
    return df

def load_silver(df: pd.DataFrame):
    client_secrets_file = os.getenv("CLIENT_SECRET")
    
    if client_secrets_file and os.path.exists(client_secrets_file):
        try:
            client = bigquery.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
        except Exception as e:
            print(f"Aviso: Erro ao carregar credenciais do JSON ({e}). Usando credenciais padrão.")
            client = bigquery.Client(project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)
        
    # Garante que o dataset 'localiza_silver' existe no BigQuery
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Converte colunas de data de nanosegundos (ns) para microsegundos (us) para que o BigQuery detecte como TIMESTAMP
    if 'dat_data_transaction' in df.columns:
        df['dat_data_transaction'] = pd.to_datetime(df['dat_data_transaction'], utc=True).astype('datetime64[us, UTC]')
    if 'dat_data_upload_bucket' in df.columns:
        df['dat_data_upload_bucket'] = pd.to_datetime(df['dat_data_upload_bucket'], utc=True).astype('datetime64[us, UTC]')

    import tempfile
    temp_dir = tempfile.gettempdir()
    temp_file = os.path.join(temp_dir, "temp_localiza_silver.parquet")
    
    print(f"Salvando DataFrame Silver temporariamente em {temp_file}...")
    df.to_parquet(temp_file, index=False)
    
    # Limpa DataFrame e força GC
    del df
    gc.collect()
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.PARQUET
    )
    
    print(f"Carregando arquivo Parquet na tabela {table_ref}...")
    with open(temp_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()
        
    # Limpa arquivo temporário
    if os.path.exists(temp_file):
        os.remove(temp_file)
    
    print(f"Carga concluída com sucesso para {table_ref}!")

if __name__ == '__main__':
    from extract_bronze import extract_bronze
    df_bronze = extract_bronze()
    df_silver = transform_silver(df_bronze)
    load_silver(df_silver)
