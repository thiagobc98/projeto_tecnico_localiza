from google.cloud import bigquery
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

PROJECT_ID = "etl-teste-tecnico"
DATASET_ID = "localiza_raw"
TABLE_ID = "raw_fraud_credit"

def extract_raw() -> pd.DataFrame:
    client_secrets_file = os.getenv("CLIENT_SECRET")
    
    if client_secrets_file and os.path.exists(client_secrets_file):
        client = bigquery.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)
        
    query = f"""
    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE date_upload_file_bucket = (
        SELECT MAX(date_upload_file_bucket) FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    )
    """
    
    dtypes = {
        'timestamp': 'string',
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
        'anomaly': 'string',
        'date_upload_file_bucket': 'datetime64[ns, UTC]'
    }
    
    print(f"Buscando dados da tabela {DATASET_ID}.{TABLE_ID} no BigQuery...")
    df = client.query(query).to_dataframe(dtypes=dtypes)
    
    return df

if __name__ == '__main__':
    df = extract_raw()
    print(f"Extração da Raw concluída! Total de linhas: {len(df)}")
    print(df.head())
