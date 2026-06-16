import pandas as pd 

# Extrai os dados do csv da pasta raw no Cloud Storage
def extract_data():
    dtypes = {
        'timestamp': 'int64',
        'sending_address': 'string',
        'receiving_address': 'string',
        'amount': 'float32',
        'transaction_type': 'category',
        'location_region': 'category',
        'ip_prefix': 'string',
        'login_frequency': 'float32',  # Usamos float32 para suportar possíveis nulos
        'session_duration': 'float32',
        'purchase_pattern': 'category',
        'age_group': 'category',
        'risk_score': 'float32',
        'anomaly': 'float32'          # Usamos float32 para suportar possíveis nulos
    }
    return pd.read_csv('gs://landing-raw/df_fraud_credit.csv', dtype=dtypes)