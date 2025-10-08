# borough_monthly.py
from pyspark.sql import SparkSession, functions as F

# 1) Spark
spark = SparkSession.builder.appName("borough-monthly").getOrCreate()

# 2) Inputs
trips = spark.read.parquet("out/processed/yellow")

zones = (
    spark.read.option("header", True).csv("taxi+_zone_lookup.csv")
    .select(
        F.col("LocationID").cast("int").alias("LocationID"),
        F.col("Borough"),
        F.col("Zone"),
    )
)

# 3) Enrich with PICKUP & DROPOFF boroughs  -------------------------------
enriched = (
    trips
    .join(
        zones.withColumnRenamed("LocationID", "PULocationID")
             .withColumnRenamed("Borough", "PUBorough")
             .withColumnRenamed("Zone", "PUZone"),
        on="PULocationID",
        how="left",
    )
    .withColumn("PUBorough", F.coalesce(F.col("PUBorough"), F.lit("Unknown")))
    .join(
        zones.withColumnRenamed("LocationID", "DOLocationID")
             .withColumnRenamed("Borough", "DOBorough")
             .withColumnRenamed("Zone", "DOZone"),
        on="DOLocationID",
        how="left",
    )
    .withColumn("DOBorough", F.coalesce(F.col("DOBorough"), F.lit("Unknown")))
)

# 4) Monthly revenue by PICKUP borough (main table) -----------------------
monthly_pickup = (
    enriched
    .groupBy("year", "month", "PUBorough")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.count("*").alias("trips"),
        F.round(F.avg("trip_distance"), 2).alias("avg_miles"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare"),
    )
)

monthly_pickup.orderBy(F.desc("revenue")).show(15, truncate=False)

(
    monthly_pickup
    .repartition("year", "month")
    .write.mode("overwrite")
    .partitionBy("year", "month")
    .parquet("out/curated/borough_monthly")
)

(
    monthly_pickup
    .orderBy(F.desc("revenue"))
    .toPandas()
    .to_csv("borough_revenue_2019.csv", index=False)
)

# 5) (Optional) PU -> DO route matrix -------------------------------------
monthly_route = (
    enriched
    .groupBy("year", "month", "PUBorough", "DOBorough")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.count("*").alias("trips"),
    )
)

# Uncomment if you want to preview and persist the routes:
# monthly_route.orderBy(F.desc("revenue")).show(15, truncate=False)
# (
#     monthly_route
#     .repartition("year", "month")
#     .write.mode("overwrite")
#     .partitionBy("year", "month")
#     .parquet("out/curated/borough_routes")
# )
# (
#     monthly_route
#     .orderBy(F.desc("revenue"))
#     .toPandas()
#     .to_csv("borough_routes_2019.csv", index=False)
# )

# 6) Done
spark.stop()
