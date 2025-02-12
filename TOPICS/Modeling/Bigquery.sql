from google.cloud import bigquery
import os
import json
import pandas as pd
import matplotlib.pyplot as plt

client = bigquery.Client(project=PROJECT_ID)
dataset_id = f"{PROJECT_ID}.public"
dataset = bigquery.Dataset(dataset_id)
client.create_dataset(dataset, exists_ok=True, timeout=30)

schema = {
    "customer": [
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("address", "STRING")
    ],
    "product": [
        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("product_name", "STRING"),
        bigquery.SchemaField("unit_price", "FLOAT64"),
        bigquery.SchemaField("description", "STRING")
    ],
    "store": [
        bigquery.SchemaField("store_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("store_name", "STRING"),
        bigquery.SchemaField("store_address", "STRING")
    ],
    "transaction": [
        bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "TIMESTAMP"),
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("store_id", "STRING")
    ],
    "sales": [
        bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("quantity", "FLOAT64")
    ]
}

for table_name in schema:
    table = bigquery.Table(f"{dataset_id}.{table_name}", schema=schema[table_name])
    client.delete_table(table, not_found_ok=True)    
    client.create_table(table)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
