import pandas as pd 

# Extrai os dados do csv da pasta raw
def extract_data():
    return pd.read_csv(fr'C:\Users\Thiago\Desktop\GitHub\projeto_tecnico_localiza\data\raw\df_fraud_credit.csv')

print(extract_data().head())