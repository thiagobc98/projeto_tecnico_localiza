from google.cloud import bigquery
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

PROJECT_ID = "etl-teste-tecnico"
DATASET_ID = "bronze"
TABLE_ID = "bronze"


def tratamento_bronze(df: pd.DataFrame) -> pd.DataFrame:
    
    # Padroniza os nomes das colunas
    df.columns = (
        df.columns
        .str.strip()              # Remove espaços no início/fim do nome
        .str.lower()              # Tudo em minúsculo
        .str.replace(' ', '_')    # Substitui espaços por underline
        .str.replace('[^\w\s]', '', regex=True) # Remove caracteres especiais (acentos, pontuações)
    )

    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

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
    'anomaly': 'anomaly'
})
    
    
    return df

def load_bronze(df: pd.DataFrame):
    client_secrets_file = os.getenv("CLIENT_SECRET")
    
    # Se houver arquivo de credenciais configurado localmente no .env, usa ele.
    # Caso contrário (como no GitHub Actions), usa a autenticação padrão do ambiente (ADC).
    if client_secrets_file and os.path.exists(client_secrets_file):
        client = bigquery.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)


    # Garante que o dataset 'bronze' existe no BigQuery
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"  # Altere para a região desejada se necessário (ex: "southamerica-east1")
    client.create_dataset(dataset_ref, exists_ok=True)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )

    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config
    )

    job.result()

    print(f"Dados carregados para {table_ref}")