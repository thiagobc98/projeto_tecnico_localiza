from google.cloud import bigquery
import pandas as pd
import os
import gc
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

PROJECT_ID = "etl-teste-tecnico"
DATASET_ID = "localiza_bronze"
TABLE_ID = "localiza_bronze"


def tratamento_bronze(df: pd.DataFrame) -> pd.DataFrame:
    # Padroniza os nomes das colunas
    df.columns = (
        df.columns
        .str.strip()              # Remove espaços no início/fim do nome
        .str.lower()              # Tudo em minúsculo
        .str.replace(' ', '_')    # Substitui espaços por underline
        .str.replace('[^\w\s]', '', regex=True) # Remove caracteres especiais (acentos, pontuações)
    )

    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['risk_score'] = pd.to_numeric(df['risk_score'], errors='coerce')
    df['login_frequency'] = pd.to_numeric(df['login_frequency'], errors='coerce')
    df['session_duration'] = pd.to_numeric(df['session_duration'], errors='coerce')
    df['anomaly'] = pd.to_numeric(df['anomaly'], errors='coerce')

    df = df.rename(columns={
        'timestamp': 'date_hour_transaction',
        'sending_address': 'address_sender',
        'receiving_address': 'address_receiver',
        'amount': 'value',
        'transaction_type': 'type_transaction',
        'location_region': 'region',
        'ip_prefix': 'ip_prefix',
        'login_frequency': 'login_frequency',
        'session_duration': 'session_duration',
        'purchase_pattern': 'purchase_pattern',
        'age_group': 'age_group',
        'risk_score': 'risk_score',
        'anomaly': 'anomaly'
    })
    
    return df

def load_bronze(df: pd.DataFrame):
    client_secrets_file = os.getenv("CLIENT_SECRET")
    
    if client_secrets_file and os.path.exists(client_secrets_file):
        client = bigquery.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)

    # Garante que o dataset 'localiza_bronze' existe no BigQuery
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Para evitar estourar a memória RAM (OOM) do worker, salvamos o dataframe em um arquivo temporário no disco,
    # limpamos o DataFrame da memória e carregamos a partir do arquivo.
    temp_file = "/tmp/temp_localiza_bronze.parquet"
    os.makedirs(os.path.dirname(temp_file), exist_ok=True)
    
    print(f"Salvando DataFrame temporariamente em {temp_file}...")
    df.to_parquet(temp_file, index=False)
    
    # Deleta DataFrame e força coleta de lixo
    del df
    gc.collect()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.PARQUET
    )

    print(f"Carregando arquivo Parquet na tabela {table_ref}...")
    with open(temp_file, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            job_config=job_config
        )
        job.result()

    # Limpa o arquivo temporário
    if os.path.exists(temp_file):
        os.remove(temp_file)

    print(f"Dados carregados para {table_ref} com sucesso!")

if __name__ == '__main__':
    # Teste local
    from extract_raw import extract_raw
    df_raw = extract_raw()
    df_treated = tratamento_bronze(df_raw)
    load_bronze(df_treated)
