# Setup
PROJECT_ID = json.load(open("auth.json","rb"))["quota_project_id"]
client = bigquery.Client(project=PROJECT_ID)
dataset_id = f"{PROJECT_ID}.ga"
src_table_id = f"{dataset_id}.google_analytics_source"
dst_table_id = f"{dataset_id}.google_analytics_agg"

# Set up the load job config
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=False,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=json.load(open("ga.json","rb"))
)

# Create dataset
dataset = bigquery.Dataset(dataset_id)
dataset = client.create_dataset(dataset, exists_ok=True, timeout=30)

# Ingestion - load raw data into BigQuery
with open("ga.csv", "rb") as source_file:
    job = client.load_table_from_file(
        source_file,
        src_table_id,
        job_config=job_config,
    )

job.result()  # Wait for the job to complete
src_table = client.get_table(src_table_id)
print("Loaded {} rows and {} columns to {}".format(src_table.num_rows, len(src_table.schema), src_table_id))

# Transformation - perform aggregation
query = """
SELECT date, COUNT(DISTINCT fullVisitorId) as num_visitors, SUM(pageviews) as num_pageviews, SUM(transactionRevenue) as num_transaction_revenue
FROM ga.google_analytics_source
GROUP BY 1
ORDER BY 1
"""
job_config = bigquery.QueryJobConfig(
    destination = dataset.table("google_analytics_agg"),
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
)
job = client.query(query, job_config=job_config)
job.result()
dst_table = client.get_table(dst_table_id)
print("Loaded {} rows and {} columns to {}".format(dst_table.num_rows, len(dst_table.schema), dst_table_id))

# Visualize
df = pd.read_gbq(query="SELECT * FROM ga.google_analytics_agg", project_id=PROJECT_ID)
df.plot.bar(x='date', y='num_visitors', rot=0)
plt.xticks(rotation=90)
plt.savefig('output/graph.png')
