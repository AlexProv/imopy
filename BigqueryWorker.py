from google.cloud import bigquery

bg_client = bigquery.Client()

query = 'select * from `imopy-analytics.housing.houses`'