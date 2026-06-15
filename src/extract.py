import pandas as pd 

# Extrai os dados do csv da pasta raw no Cloud Storage
def extract_data():
    return pd.read_csv('gs://landing-raw/df_fraud_credit.csv')