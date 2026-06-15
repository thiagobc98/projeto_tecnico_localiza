from google.cloud import bigquery
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

PROJECT_ID = "etl-teste-tecnico"
DATASET_ID = "bronze"
TABLE_ID = "transactions_raw"

def load_bronze(df: pd.DataFrame):
    client_secrets_file = os.getenv("CLIENT_SECRET")
    
    # Se houver arquivo de credenciais configurado localmente no .env, usa ele.
    # Caso contrário (como no GitHub Actions), usa a autenticação padrão do ambiente (ADC).
    if client_secrets_file and os.path.exists(client_secrets_file):
        client = bigquery.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)


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