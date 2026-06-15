import sys
import os

# Adiciona o diretório atual ao sys.path para garantir que os imports funcionem
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extract import extract_data
from load_bronze import load_bronze

def run_pipeline():
    print("Iniciando pipeline de extração...")
    df = extract_data()
    print(f"Dados extraídos com sucesso! Total de linhas: {len(df)}")

    print("Iniciando tratamento dos dados...")
    df = tratamento_bronze(df)
    print(f"Dados tratados com sucesso! Total de linhas: {len(df)}")
    
    print("Iniciando carga no BigQuery...")
    load_bronze(df)
    print("Pipeline finalizado com sucesso!")

if __name__ == '__main__':
    run_pipeline()
