from google.cloud import bigquery
import os
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()

PROJECT_ID = "etl-teste-tecnico"
DATASET_ID = "localiza_gold"

def get_client():
    client_secrets_file = os.getenv("CLIENT_SECRET")
    # Autenticação híbrida
    if client_secrets_file and os.path.exists(client_secrets_file):
        try:
            return bigquery.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
        except Exception as e:
            print(f"Aviso: Erro ao carregar credenciais do JSON ({e}). Usando credenciais padrão.")
            return bigquery.Client(project=PROJECT_ID)
    else:
        return bigquery.Client(project=PROJECT_ID)

def load_gold_tabela_1():
    client = get_client()

    # Garante que o dataset 'localiza_gold' existe no BigQuery
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    
    print("Iniciando processamento da Gold 1 (region_risk_average)...")
    
    # Query 1: region por média de risk_score (usando nomes em português)
    query_gold1 = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.region_risk_average` AS
    SELECT 
      des_regiao AS region, 
      AVG(SAFE_CAST(vlr_score_risco AS FLOAT64)) AS average_risk_score 
    FROM `{PROJECT_ID}.localiza_silver.localiza_silver` 
    GROUP BY 1 
    ORDER BY average_risk_score DESC
    """
    
    print("Executando carga para localiza_gold.region_risk_average...")
    query_job = client.query(query_gold1)
    query_job.result()
    print("Tabela localiza_gold.region_risk_average criada com sucesso!")

def load_gold_tabela_2():
    client = get_client()

    # Garante que o dataset 'localiza_gold' existe no BigQuery
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    
    print("Iniciando processamento da Gold 2 (top_receiving_addresses_sales)...")
    
    # Query 2: Transação 'sale' mais recente por receiving address (usando nomes em português), top 3 com maior value
    query_gold2 = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.top_receiving_addresses_sales` AS
    WITH ranked_sales AS (
      SELECT 
        cod_endereco_recebido AS address_receiver,
        vlr_valor AS value,
        dat_data_transaction AS date_hour_transaction,
        ROW_NUMBER() OVER(PARTITION BY cod_endereco_recebido ORDER BY dat_data_transaction DESC) as rn
      FROM `{PROJECT_ID}.localiza_silver.localiza_silver`
      WHERE des_tipo_transacao = 'sale'
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
    
    print("Executando carga para localiza_gold.top_receiving_addresses_sales...")
    query_job = client.query(query_gold2)
    query_job.result()
    print("Tabela localiza_gold.top_receiving_addresses_sales criada com sucesso!")

def load_gold():
    """Função compatível com chamadas legadas que executa ambos os processos."""
    load_gold_tabela_1()
    load_gold_tabela_2()

if __name__ == '__main__':
    load_gold()
