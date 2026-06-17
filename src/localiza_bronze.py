from google.cloud import bigquery
import pandas as pd
import os
import gc
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = "etl-teste-tecnico"
DATASET_ID = "localiza_bronze"
TABLE_ID = "localiza_bronze"


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
    client_secrets_file = os.getenv("CLIENT_SECRET")
    
    if client_secrets_file and os.path.exists(client_secrets_file):
        try:
            client = bigquery.Client.from_service_account_json(client_secrets_file, project=PROJECT_ID)
        except Exception as e:
            print(f"Aviso: Erro ao carregar credenciais do JSON ({e}). Usando credenciais padrão.")
            client = bigquery.Client(project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)

    # Garante que o dataset 'localiza_bronze' existe no BigQuery
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Converte colunas de data de nanosegundos  para microsegundos para que o BigQuery detecte como TIMESTAMP
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

if __name__ == '__main__':
    # Teste local
    from extract_raw import extract_raw
    df_raw = extract_raw()
    df_treated = tratamento_bronze(df_raw)
    load_bronze(df_treated)
