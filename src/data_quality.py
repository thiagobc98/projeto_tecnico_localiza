from google.cloud import bigquery
import os
import datetime
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = "etl-teste-tecnico"
DATASET_ID = "localiza_quality"
TABLE_ID = "data_quality_report"

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

def run_data_quality_checks():
    client = get_client()

    # 1. Garante que o dataset localiza_quality existe
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)

    # 2. Garante que a tabela data_quality_report existe
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    schema = [
        bigquery.SchemaField("data_execucao", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("tabela", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("coluna", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("regra", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("qtd_registros", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("qtd_erros", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("percentual_erro", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("percentual_conformidade", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("descricao_erro", "STRING", mode="NULLABLE"),
    ]
    
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table, exists_ok=True)

    print("Iniciando validação de Data Quality...")
    
    # 3. Definição das regras de validação por tabela
    # Cada regra mapeia um nome amigável para a condição SQL que representa o erro
    tables_rules = {
        "localiza_bronze.localiza_bronze": [
            {
                "coluna": "dat_data_transaction",
                "regra": "not_null",
                "condition": "dat_data_transaction IS NULL",
                "descricao_erro": "Data de transação ausente",
                "critical": True
            },
            {
                "coluna": "vlr_valor",
                "regra": "not_null",
                "condition": "vlr_valor IS NULL",
                "descricao_erro": "Valor de transação ausente",
                "critical": True
            },
            {
                "coluna": "cod_endereco_recebido",
                "regra": "not_null",
                "condition": "cod_endereco_recebido IS NULL",
                "descricao_erro": "Endereço recebedor ausente",
                "critical": False
            }
        ],
        "localiza_silver.localiza_silver": [
            {
                "coluna": "dat_data_transaction",
                "regra": "not_null",
                "condition": "dat_data_transaction IS NULL",
                "descricao_erro": "Data de transação ausente",
                "critical": True
            },
            {
                "coluna": "vlr_valor",
                "regra": "non_negative_and_not_null",
                "condition": "vlr_valor IS NULL OR vlr_valor < 0",
                "descricao_erro": "Valor de transação nulo ou negativo",
                "critical": True
            },
            {
                "coluna": "des_regiao",
                "regra": "valid_region",
                "condition": "des_regiao IS NULL OR des_regiao IN ('0', '', 'nan')",
                "descricao_erro": "Região ausente ou inválida",
                "critical": False
            },
            {
                "coluna": "vlr_score_risco",
                "regra": "not_null",
                "condition": "vlr_score_risco IS NULL",
                "descricao_erro": "Score de risco ausente",
                "critical": True
            },
            {
                "coluna": "cod_endereco_enviado",
                "regra": "not_null",
                "condition": "cod_endereco_enviado IS NULL",
                "descricao_erro": "Endereço remetente ausente",
                "critical": True
            },
            {
                "coluna": "cod_endereco_recebido",
                "regra": "not_null",
                "condition": "cod_endereco_recebido IS NULL",
                "descricao_erro": "Endereço recebedor ausente",
                "critical": True
            }
        ]
    }

    report_rows = []
    has_failures = False
    now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # 4. Executa a validação de forma otimizada (uma única query por tabela)
    for table_name, rules in tables_rules.items():
        print(f"Executando regras de qualidade para {table_name}...")
        
        # Monta a query dinâmica
        select_parts = ["COUNT(*) as total_rows"]
        for idx, rule in enumerate(rules):
            select_parts.append(f"COUNTIF({rule['condition']}) as err_{idx}")
            
        query = f"SELECT {', '.join(select_parts)} FROM `{PROJECT_ID}.{table_name}`"
        
        try:
            query_result = list(client.query(query).result())
            if not query_result:
                print(f"Aviso: Tabela {table_name} vazia ou inacessível. Pulando regras.")
                continue
                
            row = query_result[0]
            total_rows = row["total_rows"]
            
            if total_rows == 0:
                print(f"Aviso: Tabela {table_name} possui 0 registros. Pulando regras.")
                continue
                
            for idx, rule in enumerate(rules):
                err_count = row[f"err_{idx}"]
                percent_error = (err_count / total_rows) * 100
                percent_compliance = 100 - percent_error
                
                # Regras críticas exigem 99% de conformidade, não críticas exigem 95%
                threshold = 99.0 if rule["critical"] else 95.0
                status = "PASS" if percent_compliance >= threshold else "FAIL"
                
                if status == "FAIL" and rule["critical"]:
                    has_failures = True
                    print(f"FALHA DQ CRÍTICA: {table_name}.{rule['coluna']} -> {rule['descricao_erro']}. Conformidade: {percent_compliance:.2f}% (Limiar: {threshold}%)")
                
                report_rows.append({
                    "data_execucao": now_str,
                    "tabela": table_name,
                    "coluna": rule["coluna"],
                    "regra": rule["regra"],
                    "qtd_registros": total_rows,
                    "qtd_erros": err_count,
                    "percentual_erro": round(percent_error, 2),
                    "percentual_conformidade": round(percent_compliance, 2),
                    "status": status,
                    "descricao_erro": rule["descricao_erro"]
                })
                
        except Exception as e:
            print(f"Erro ao auditar tabela {table_name}: {e}")
            has_failures = True

    # 5. Insere os resultados na tabela de auditoria
    if report_rows:
        print(f"Gravando {len(report_rows)} registros de auditoria em {table_ref}...")
        errors = client.insert_rows_json(table_ref, report_rows)
        if errors:
            print(f"Erro ao inserir linhas de qualidade: {errors}")
            raise RuntimeError(f"Falha ao salvar auditoria de Data Quality: {errors}")
        print("Auditoria gravada com sucesso!")
    else:
        print("Nenhum registro de auditoria gerado.")

    # 6. Se houve falha crítica de qualidade de dados, lança erro para falhar a DAG
    if has_failures:
        raise ValueError("O pipeline falhou nos testes de Data Quality. Verifique a tabela localiza_quality.data_quality_report.")

if __name__ == '__main__':
    run_data_quality_checks()
