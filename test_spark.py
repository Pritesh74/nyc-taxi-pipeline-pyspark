from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("SparkTest").getOrCreate()

# Create a small DataFrame
data = [("Alice", 23), ("Bob", 34), ("Charlie", 29)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show DataFrame content
df.show()

# Stop Spark
spark.stop()
