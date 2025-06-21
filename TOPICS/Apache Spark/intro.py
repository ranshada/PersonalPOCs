print("hi")

# Reading a CSV file into a PySpark DataFrame
df = spark.read.csv("/path/to/your/fancy_file.csv", header=True, inferSchema=True)

# DataFrames Are Lazy... But in a Good Way 🛌
# Here’s the thing: PySpark DataFrames are like a friend who promises to do the dishes but waits until the very last second. They are lazy, which means they don’t actually do any work until you tell them to. This is called lazy evaluation.

# Example: When you ask PySpark to do something, like filter some data:

# # Filter rows where age is greater than 30
# df_filtered = df.filter(df['age'] > 30)
# Nothing happens. Nada. Zilch. PySpark is chillin’. 🧘‍♂️

# But when you force it to act (by using an action, like .show() or .collect()), that’s when it rolls up its sleeves and does the work.

# # Now Spark gets off the couch and does something!
# df_filtered.show()

# Show the first few rows of the DataFrame
df.show(5)