import sys
import os

# Adiciona o diretório atual ao sys.path para garantir que os imports funcionem
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extract import extract_data
from load_bronze import load_bronze, tratamento_bronze
from load_silver import load_silver, transform_silver

def run_pipeline():
    print("Iniciando pipeline de extração...")
    df = extract_data()
    print(f"Dados extraídos com sucesso! Total de linhas: {len(df)}")

    print("Iniciando tratamento dos dados para Bronze...")
    df_bronze = tratamento_bronze(df)
    print(f"Dados tratados para Bronze com sucesso! Total de linhas: {len(df_bronze)}")
    
    print("Iniciando carga na camada Bronze do BigQuery...")
    load_bronze(df_bronze)
    
    print("Iniciando processamento para a camada Silver...")
    df_silver = transform_silver(df_bronze)
    
    print("Iniciando carga na camada Silver do BigQuery...")
    load_silver(df_silver)
    
    print("Pipeline finalizado com sucesso!")

if __name__ == '__main__':
    run_pipeline()
