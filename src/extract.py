import pandas as pd 

# Extrai os dados do csv da pasta raw no Cloud Storage
def extract_data():
    dtypes = {
        'timestamp': 'int64',
        'sending_address': 'string',
        'receiving_address': 'string',
        'amount': 'string',
        'transaction_type': 'category',
        'location_region': 'category',
        'ip_prefix': 'string',
        'login_frequency': 'string',
        'session_duration': 'string',
        'purchase_pattern': 'category',
        'age_group': 'category',
        'risk_score': 'string',
        'anomaly': 'string'
    }
    return pd.read_csv('gs://landing-raw/df_fraud_credit.csv', dtype=dtypes, na_values=['none', 'None', 'NaN', 'null', ''])