import pandas as pd

df = pd.read_csv("ga.csv")
print(df.head())


import pandas as pd

df = pd.read_csv("ga.csv", usecols=['visitId', 'transactionRevenue'])
print(df.head())

df = pd.read_csv("ga.csv", usecols=lambda x: x in ['visitId', 'transactionRevenue'])
print(df.head())


import pandas as pd
iter_csv = pd.read_csv('ga.csv', iterator=True, chunksize=100)
df = pd.concat([chunk[chunk['transactionRevenue'] > 0] for chunk in iter_csv])
print(df.head())



from google.cloud import bigquery

client = bigquery.Client(project=PROJECT_ID)
sql = """
    SELECT visitId, date, totals.pageviews, totals.transactionRevenue
    FROM bigquery-public-data.google_analytics_sample.ga_sessions_20170801
    WHERE totals.transactionRevenue > 0
"""
df = client.query(sql).to_dataframe()
print(df.head())


from google.cloud import bigquery

client = bigquery.Client(project=PROJECT_ID)
sql = """
    SELECT visitId, date, totals.pageviews, totals.transactionRevenue
    FROM bigquery-public-data.google_analytics_sample.ga_sessions_20170801
    WHERE totals.transactionRevenue > 0
"""
df = client.query(sql).to_dataframe()
print(df.head())




import datetime

from google.cloud import bigquery
import pandas as pd
import pytz
import json

table_id = f"{dataset_id}.ga_csv"
job_config = bigquery.LoadJobConfig(
    schema = [
        bigquery.SchemaField("visitId", "INTEGER"),
        bigquery.SchemaField("transactionRevenue", "FLOAT"),  
    ],
    autodetect=False,
    write_disposition="WRITE_TRUNCATE",
)

dataframe = pd.read_csv("ga.csv", usecols=lambda x: x in ['visitId', 'transactionRevenue'])
job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
job.result()

table = client.get_table(table_id)
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)