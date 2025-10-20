from pyspark.sql import SparkSession

# This is your "Head Chef" setting up the kitchen
spark = SparkSession.builder \
    .appName("MyFirstSparkProgram") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# The Head Chef (Driver) coordinates everything
print(f"Driver is running on: {spark.sparkContext.getConf().get('spark.driver.host')}")