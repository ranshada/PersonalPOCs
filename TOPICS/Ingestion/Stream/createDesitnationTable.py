client = bigquery.Client(project=PROJECT_ID)
dataset = f"{PROJECT_ID}.ingestion"
dataset = client.create_dataset(dataset, exists_ok=True, timeout=30)

# create table 
table_ref = dataset.table("bitcoin_transactions_stream")
schema = [
    bigquery.SchemaField("hash", "STRING"),
    bigquery.SchemaField("size", "INTEGER"),
    bigquery.SchemaField("block_hash", "STRING"),
    bigquery.SchemaField("block_number", "INTEGER"),
    bigquery.SchemaField("block_timestamp", "INTEGER"),
]
table = bigquery.Table(table_ref, schema=schema)
client.delete_table(table, not_found_ok=True)
table = client.create_table(table, exists_ok=True)
print(f"{table} has been created.")