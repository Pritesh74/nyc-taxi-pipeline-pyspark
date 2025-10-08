from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Define schema for the 2019 yellow taxi data
schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
])

# Initialize Spark
spark = SparkSession.builder.appName("yellow2019-preview").getOrCreate()

# Read all CSV files
df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv("yellow_tripdata_2019-0*.csv")  # make sure these files are in the same folder
)

print("\n✅ Successfully read data!")
print("Total Rows:", df.count())
print("Schema:")
df.printSchema()

print("\nSample rows:")
df.show(5, truncate=False)

spark.stop()
