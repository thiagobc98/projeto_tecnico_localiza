from google.cloud import bigquery
import pandas as pd
import os
import gc
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = "etl-teste-tecnico"
DATASET_ID = "localiza_silver"
TABLE_ID = "localiza_silver"

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

def transform_silver(df: pd.DataFrame) -> pd.DataFrame:
    print("Iniciando limpeza e transformações para a camada Silver (nomes em português)...")
    
    # Remove duplicatas baseadas na transação
    linhas_antes = len(df)
    df = df.drop_duplicates(
        subset=['dat_data_transaction', 'cod_endereco_enviado', 'cod_endereco_recebido']
    )
    print(f"Duplicatas removidas: {linhas_antes - len(df)} linhas.")
    
    # Remove nulos nas colunas essenciais
    df = df.dropna(subset=['dat_data_transaction', 'cod_endereco_enviado', 'cod_endereco_recebido', 'vlr_valor'])

    # Filtra transações com valores negativos 
    df = df[df['vlr_valor'] >= 0]
    
    # Substitui valores '0' ou 0 na coluna 'des_regiao' por NA
    df['des_regiao'] = df['des_regiao'].replace(['0', 0], pd.NA)
    
    # Padroniza strings para minúsculo nas colunas
    for col in df.select_dtypes(include=['object', 'category', 'string']).columns:
        df[col] = df[col].astype(str).str.strip().str.lower()
            
    print(f"Transformações concluídas! Total de linhas para Silver: {len(df)}")
    return df

def load_silver(df: pd.DataFrame):
    client = get_client()
        
    # Garante que o dataset 'localiza_silver' existe no BigQuery
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Converte colunas de data de nanosegundos (ns) para microsegundos (us) para que o BigQuery detecte como TIMESTAMP
    if 'dat_data_transaction' in df.columns:
        df['dat_data_transaction'] = pd.to_datetime(df['dat_data_transaction'], utc=True).astype('datetime64[us, UTC]')
    if 'dat_data_upload_bucket' in df.columns:
        df['dat_data_upload_bucket'] = pd.to_datetime(df['dat_data_upload_bucket'], utc=True).astype('datetime64[us, UTC]')

    import tempfile
    temp_dir = tempfile.gettempdir()
    temp_file = os.path.join(temp_dir, "temp_localiza_silver.parquet")
    
    print(f"Salvando DataFrame Silver temporariamente em {temp_file}...")
    df.to_parquet(temp_file, index=False)
    
    # Limpa DataFrame e força GC
    del df
    gc.collect()
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.PARQUET
    )
    
    print(f"Carregando arquivo Parquet na tabela {table_ref}...")
    with open(temp_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()
        
    # Limpa arquivo temporário
    if os.path.exists(temp_file):
        os.remove(temp_file)
    
    print(f"Carga concluída com sucesso para {table_ref}!")

def load_silver_bq():
    client = get_client()
    
    # Garante que o dataset 'localiza_silver' existe no BigQuery
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
    WITH raw_bronze AS (
      SELECT
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
        dat_data_upload_bucket,
        ROW_NUMBER() OVER(
          PARTITION BY dat_data_transaction, cod_endereco_enviado, cod_endereco_recebido 
          ORDER BY dat_data_upload_bucket DESC
        ) as rn
      FROM `{PROJECT_ID}.localiza_bronze.localiza_bronze`
      WHERE dat_data_upload_bucket = (
        SELECT MAX(dat_data_upload_bucket) FROM `{PROJECT_ID}.localiza_bronze.localiza_bronze`
      )
    )
    SELECT
      dat_data_transaction,
      LOWER(TRIM(cod_endereco_enviado)) AS cod_endereco_enviado,
      LOWER(TRIM(cod_endereco_recebido)) AS cod_endereco_recebido,
      vlr_valor,
      LOWER(TRIM(des_tipo_transacao)) AS des_tipo_transacao,
      CASE 
        WHEN TRIM(des_regiao) = '0' OR TRIM(des_regiao) = '' THEN NULL 
        ELSE LOWER(TRIM(des_regiao)) 
      END AS des_regiao,
      LOWER(TRIM(vlr_ip_prefixo)) AS vlr_ip_prefixo,
      vlr_login_frequencia,
      vlr_duracao_sessao,
      LOWER(TRIM(des_comportamento_compra)) AS des_comportamento_compra,
      LOWER(TRIM(des_faixa_etaria)) AS des_faixa_etaria,
      vlr_score_risco,
      LOWER(TRIM(des_categoria_risco)) AS des_categoria_risco,
      dat_data_upload_bucket
    FROM raw_bronze
    WHERE rn = 1
      AND dat_data_transaction IS NOT NULL
      AND cod_endereco_enviado IS NOT NULL
      AND cod_endereco_recebido IS NOT NULL
      AND vlr_valor IS NOT NULL
      AND vlr_valor >= 0
    """
    
    print("Iniciando processamento e carga da camada Silver via SQL...")
    query_job = client.query(query)
    query_job.result()
    print(f"Carga da camada Silver concluída com sucesso para {table_ref}!")

if __name__ == '__main__':
    # Teste local via BQ
    load_silver_bq()
