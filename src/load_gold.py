from google.cloud import bigquery
import os
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()

PROJECT_ID = "etl-teste-tecnico"
DATASET_ID = "gold"

def load_gold():
    client_secrets_file = os.getenv("CLIENT_SECRET")
    
    # Autenticação híbrida
    if client_secrets_file and os.path.exists(client_secrets_file):
        client = bigquery.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)

    # Garante que o dataset 'gold' existe no BigQuery
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    
    print("Iniciando processamento da camada Gold no BigQuery com SQL...")
    
    # Query 1: region por média de risk_score
    query_gold1 = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.region_risk_average` AS
    SELECT 
      region, 
      AVG(SAFE_CAST(risk_score AS FLOAT64)) AS avg_risk_score 
    FROM `{PROJECT_ID}.silver.silver` 
    GROUP BY region 
    ORDER BY avg_risk_score DESC
    """
    
    # Query 2: Transação 'sale' mais recente por receiving address (address_receiver), top 3 com maior value
    query_gold2 = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.top_receiving_addresses_sales` AS
    WITH ranked_sales AS (
      SELECT 
        address_receiver,
        value,
        date_hour_transaction,
        ROW_NUMBER() OVER(PARTITION BY address_receiver ORDER BY date_hour_transaction DESC) as rn
      FROM `{PROJECT_ID}.silver.silver`
      WHERE type_transaction = 'sale'
    )
    SELECT 
      address_receiver,
      value,
      date_hour_transaction
    FROM ranked_sales
    WHERE rn = 1
    ORDER BY value DESC
    LIMIT 3
    """
    
    print("Executando carga para gold.region_risk_average...")
    query_job1 = client.query(query_gold1)
    query_job1.result()
    print("Tabela gold.region_risk_average criada com sucesso!")
    
    print("Executando carga para gold.top_receiving_addresses_sales...")
    query_job2 = client.query(query_gold2)
    query_job2.result()
    print("Tabela gold.top_receiving_addresses_sales criada com sucesso!")

if __name__ == '__main__':
    load_gold()
