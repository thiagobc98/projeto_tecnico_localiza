from google.cloud import bigquery
import pandas as pd
import os
from dotenv import load_dotenv

# Carrega as variáveis de ambiente
load_dotenv()

PROJECT_ID = "etl-teste-tecnico"
DATASET_ID = "silver"
TABLE_ID = "silver"

def transform_silver(df: pd.DataFrame) -> pd.DataFrame:
    print("Iniciando limpeza e transformações para a camada Silver...")
    
    # Remove duplicatas
    linhas_antes = len(df)
    # df = df.drop_duplicates()
    df = df.drop_duplicates(
        subset=['date_hour_transaction','address_sender','address_receiver']
    )
    print(f"Duplicatas removidas: {linhas_antes - len(df)} linhas.")
    
    # Remove nulos nas colunas
    df = df.dropna(subset=['date_hour_transaction', 'address_sender', 'address_receiver', 'value'])

    # Filtra transações com valores negativos 
    df = df[df['value'] >= 0]
    
    # Substitui valores 0 na coluna 'region' por NA
    df['region'] = df['region'].replace('0', pd.NA)
    
    # Padroniza strings para minúsculo nas colunas categóricas
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip().str.lower()
            
    print(f"Transformações concluídas! Total de linhas para Silver: {len(df)}")
    return df

def load_silver(df: pd.DataFrame):
    client_secrets_file = os.getenv("CLIENT_SECRET")
    
    # Autenticação híbrida (local JSON ou automático no GitHub Actions)
    if client_secrets_file and os.path.exists(client_secrets_file):
        client = bigquery.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)
        
    # Garante que o dataset 'silver' existe no BigQuery
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )
    
    print(f"Carregando dados na tabela {table_ref} no BigQuery...")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    
    print(f"Carga concluída com sucesso para {table_ref}!")

if __name__ == '__main__':
    # Bloco para testar localmente caso execute o script direto
    from extract_bronze import extract_bronze
    df_bronze = extract_bronze()
    df_silver = transform_silver(df_bronze)
    load_silver(df_silver)
