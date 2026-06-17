from google.cloud import bigquery
import pandas as pd
import os
import gc
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = "etl-teste-tecnico"
DATASET_ID = "localiza_bronze"
TABLE_ID = "localiza_bronze"

def get_client():
    client_secrets_file = os.getenv("CLIENT_SECRET")
    if client_secrets_file and os.path.exists(client_secrets_file):
        try:
            return bigquery.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
        except Exception as e:
            print(f"Aviso: Erro ao carregar credenciais do JSON ({e}). Usando credenciais padrão.")
            return bigquery.Client(project=PROJECT_ID)
    else:
        return bigquery.Client(project=PROJECT_ID)

def tratamento_bronze(df: pd.DataFrame) -> pd.DataFrame:
    # Padroniza os nomes das colunas
    df.columns = (
        df.columns
        .str.strip()              # Remove espaços no início/fim do nome
        .str.lower()              # Tudo em minúsculo
        .str.replace(' ', '_')    # Substitui espaços por underline
        .str.replace('[^\w\s]', '', regex=True) # Remove caracteres especiais
    )

    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['risk_score'] = pd.to_numeric(df['risk_score'], errors='coerce')
    df['login_frequency'] = pd.to_numeric(df['login_frequency'], errors='coerce')
    df['session_duration'] = pd.to_numeric(df['session_duration'], errors='coerce')
    
    df = df.rename(columns={
        'timestamp': 'dat_data_transaction',
        'sending_address': 'cod_endereco_enviado',
        'receiving_address': 'cod_endereco_recebido',
        'amount': 'vlr_valor',
        'transaction_type': 'des_tipo_transacao',
        'location_region': 'des_regiao',
        'ip_prefix': 'vlr_ip_prefixo',
        'login_frequency': 'vlr_login_frequencia',
        'session_duration': 'vlr_duracao_sessao',
        'purchase_pattern': 'des_comportamento_compra',
        'age_group': 'des_faixa_etaria',
        'risk_score': 'vlr_score_risco',
        'anomaly': 'des_categoria_risco',
        'date_upload_file_bucket': 'dat_data_upload_bucket'
    })
    
    return df

def load_bronze(df: pd.DataFrame):
    client = get_client()

    # Garante que o dataset 'localiza_bronze' existe no BigQuery
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Converte colunas de data de nanosegundos para microsegundos para que o BigQuery detecte como TIMESTAMP
    if 'dat_data_transaction' in df.columns:
        df['dat_data_transaction'] = pd.to_datetime(df['dat_data_transaction'], utc=True).astype('datetime64[us, UTC]')
    if 'dat_data_upload_bucket' in df.columns:
        df['dat_data_upload_bucket'] = pd.to_datetime(df['dat_data_upload_bucket'], utc=True).astype('datetime64[us, UTC]')

    import tempfile
    temp_dir = tempfile.gettempdir()
    temp_file = os.path.join(temp_dir, "temp_localiza_bronze.parquet")
    
    print(f"Salvando DataFrame temporariamente em {temp_file}...")
    df.to_parquet(temp_file, index=False)
    
    del df
    gc.collect()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
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

    if os.path.exists(temp_file):
        os.remove(temp_file)

    print(f"Dados carregados para {table_ref} com sucesso!")

def load_bronze_bq():
    client = get_client()
    
    # Garante que o dataset 'localiza_bronze' existe no BigQuery
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_ref}` (
      dat_data_transaction     TIMESTAMP,
      cod_endereco_enviado     STRING,
      cod_endereco_recebido    STRING,
      vlr_valor                NUMERIC,
      des_tipo_transacao       STRING,
      des_regiao               STRING,
      vlr_ip_prefixo           STRING,
      vlr_login_frequencia     NUMERIC,
      vlr_duracao_sessao       NUMERIC,
      des_comportamento_compra STRING,
      des_faixa_etaria         STRING,
      vlr_score_risco          FLOAT64,
      des_categoria_risco      STRING,
      dat_data_upload_bucket   TIMESTAMP
    )
    """
    client.query(create_table_query).result()

    query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` (
      dat_data_transaction,
      cod_endereco_enviado,
      cod_endereco_recebido,
      vlr_valor,
      des_tipo_transacao,
      des_regiao,
      vlr_ip_prefixo,
      vlr_login_frequencia,
      vlr_duracao_sessao,
      des_comportamento_compra,
      des_faixa_etaria,
      vlr_score_risco,
      des_categoria_risco,
      dat_data_upload_bucket
    )
    SELECT
      TIMESTAMP_SECONDS(SAFE_CAST(timestamp AS INT64)) AS dat_data_transaction,
      sending_address AS cod_endereco_enviado,
      receiving_address AS cod_endereco_recebido,
      SAFE_CAST(amount AS NUMERIC) AS vlr_valor,
      transaction_type AS des_tipo_transacao,
      CASE WHEN TRIM(location_region) = '0' OR TRIM(location_region) = '' THEN NULL ELSE location_region END AS des_regiao,
      ip_prefix AS vlr_ip_prefixo,
      SAFE_CAST(login_frequency AS NUMERIC) AS vlr_login_frequencia,
      SAFE_CAST(session_duration AS NUMERIC) AS vlr_duracao_sessao,
      purchase_pattern AS des_comportamento_compra,
      age_group AS des_faixa_etaria,
      SAFE_CAST(risk_score AS FLOAT64) AS vlr_score_risco,
      anomaly AS des_categoria_risco,
      date_upload_file_bucket AS dat_data_upload_bucket
    FROM `{PROJECT_ID}.localiza_raw.raw_fraud_credit`
    WHERE date_upload_file_bucket = (
        SELECT MAX(date_upload_file_bucket) FROM `{PROJECT_ID}.localiza_raw.raw_fraud_credit`
    )
    """
    
    print("Iniciando processamento e carga da camada Bronze via SQL...")
    query_job = client.query(query)
    query_job.result()
    print(f"Carga da camada Bronze concluída com sucesso para {table_ref}!")

if __name__ == '__main__':
    # Teste local via BQ
    load_bronze_bq()
