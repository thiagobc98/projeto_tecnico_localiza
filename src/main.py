import sys
import os
import gc
import functions_framework

# Adiciona o diretório atual ao sys.path para garantir que os imports funcionem
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from localiza_raw import load_raw
from extract_raw import extract_raw
from localiza_bronze import load_bronze, tratamento_bronze
from localiza_silver import load_silver, transform_silver
from localiza_gold import load_gold

def run_pipeline():
    # 1. Executa a camada RAW (Lê do GCS, adiciona timestamp e escreve na tabela RAW do BigQuery)
    load_raw()
    gc.collect()

    # 2. Extrai dados da camada RAW do BigQuery para iniciar a Bronze
    print("Iniciando pipeline de extração para a camada Bronze...")
    df_raw = extract_raw()
    print(f"Dados extraídos da camada RAW com sucesso! Total de linhas: {len(df_raw)}")

    print("Iniciando tratamento dos dados para Bronze...")
    df_bronze = tratamento_bronze(df_raw)
    print(f"Dados tratados para Bronze com sucesso! Total de linhas: {len(df_bronze)}")
    
    # Liberar memória do df bruto
    del df_raw
    gc.collect()
    
    print("Iniciando carga na camada Bronze do BigQuery...")
    load_bronze(df_bronze)
    
    # Liberar df_bronze após o load
    gc.collect()
    
    print("Iniciando processamento para a camada Silver...")
    # Busca os dados da Bronze no BigQuery para o dataframe local
    from extract_bronze import extract_bronze
    df_bronze_loaded = extract_bronze()
    
    df_silver = transform_silver(df_bronze_loaded)
    del df_bronze_loaded
    gc.collect()
    
    print("Iniciando carga na camada Silver do BigQuery...")
    load_silver(df_silver)
    
    gc.collect()
    
    print("Iniciando processamento e carga da camada Gold do BigQuery...")
    load_gold()
    
    print("Pipeline finalizado com sucesso!")

@functions_framework.http
def run_etl_cf(request):
    """Ponto de entrada (Entrypoint) para o GCP Cloud Functions (Gatilho HTTP)."""
    try:
        run_pipeline()
        return "Pipeline ETL executado com sucesso!", 200
    except Exception as e:
        import traceback
        err = f"Erro na execução do pipeline: {str(e)}\n{traceback.format_exc()}"
        print(err)
        return err, 500

if __name__ == '__main__':
    run_pipeline()
