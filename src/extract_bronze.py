from google.cloud import bigquery
import pandas as pd
import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

PROJECT_ID = "etl-teste-tecnico"
DATASET_ID = "localiza_bronze"
TABLE_ID = "localiza_bronze"

def extract_bronze() -> pd.DataFrame:
    client_secrets_file = os.getenv("CLIENT_SECRET")
    
    # Se houver arquivo de credenciais local no .env, usa ele.
    # Caso contrário (no GitHub Actions), usa a autenticação automática do ambiente.
    if client_secrets_file and os.path.exists(client_secrets_file):
        client = bigquery.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)
        
    # Extrai apenas as linhas do lote mais recente
    query = f"""
    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE dat_data_upload_bucket = (
        SELECT MAX(dat_data_upload_bucket) FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    )
    """
    
    # Dtypes mapeados com os nomes em português da camada Bronze
    dtypes = {
        'dat_data_transaction': 'datetime64[ns, UTC]',
        'cod_endereco_enviado': 'string',
        'cod_endereco_recebido': 'string',
        'vlr_valor': 'float32',
        'des_tipo_transacao': 'category',
        'des_regiao': 'category',
        'vlr_ip_prefixo': 'string',
        'vlr_login_frequencia': 'float32',
        'vlr_duracao_sessao': 'float32',
        'des_comportamento_compra': 'category',
        'des_faixa_etaria': 'category',
        'vlr_score_risco': 'float32',
        'des_categoria_risco': 'string',  # anomaly virou string
        'dat_data_upload_bucket': 'datetime64[ns, UTC]'
    }
    
    print(f"Buscando dados da tabela {DATASET_ID}.{TABLE_ID} no BigQuery...")
    df = client.query(query).to_dataframe(dtypes=dtypes)
    
    return df

if __name__ == '__main__':
    df = extract_bronze()
    print(f"Extração concluída! Total de linhas: {len(df)}")
    print(df.head())
