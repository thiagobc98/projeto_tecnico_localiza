from google.cloud import bigquery
from google_auth_oauthlib.flow import InstalledAppFlow
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
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    
    flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, scopes=scopes)
    credentials = flow.run_local_server(port=0)
    
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)


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