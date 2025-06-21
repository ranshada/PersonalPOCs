# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

# Load the customers.csv dataset
#df = spark.read.format('csv').options(header=True, inferSchema=True).load('/samples/customers.csv')
df=spark.read.csv('/samples/customers.csv', header=True, inferSchema=True)
#df.printSchema()
df.show(5)

df=spark.read.csv('/samples/customers.csv', header=True, inferSchema=True)
#df.printSchema()
#df.select("first_name", "last_name", "address").show()

df.select(df.first_name, df.last_name, df.address).show(1)

df.select(df.columns[1:5]).show()
#

df.printSchema()
df.show(5)
df.select(df.first_name, df.last_name, df.address).show(1)

df.withColumn("old_address", df.customer_id*10).show()

df.withColumn("Country",lit("USA")).show()

df.withColumn("Zero", lit(0)).show()