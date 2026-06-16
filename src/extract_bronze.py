from google.cloud import bigquery
import pandas as pd
import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

PROJECT_ID = "etl-teste-tecnico"
DATASET_ID = "bronze"
TABLE_ID = "bronze"

def extract_bronze() -> pd.DataFrame:
    client_secrets_file = os.getenv("CLIENT_SECRET")
    
    # Se houver arquivo de credenciais local no .env, usa ele.
    # Caso contrário (no GitHub Actions), usa a autenticação automática do ambiente.
    if client_secrets_file and os.path.exists(client_secrets_file):
        client = bigquery.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)
        
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
    
    dtypes = {
        'date_hour_transaction': 'datetime64[ns, UTC]',
        'address_sender': 'string',
        'address_receiver': 'string',
        'value': 'float32',
        'type_transaction': 'category',
        'region': 'category',
        'ip_prefix': 'string',
        'login_frequency': 'float32',
        'session_duration': 'float32',
        'purchase_pattern': 'category',
        'age_group': 'category',
        'risk_score': 'float32',
        'anomaly': 'float32'
    }
    
    print(f"Buscando dados da tabela {DATASET_ID}.{TABLE_ID} no BigQuery...")
    df = client.query(query).to_dataframe(dtypes=dtypes)
    
    return df

if __name__ == '__main__':
    df = extract_bronze()
    print(f"Extração concluída! Total de linhas: {len(df)}")
    print(df.head())
