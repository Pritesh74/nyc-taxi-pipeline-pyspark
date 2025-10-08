from pyspark.sql import functions as F

def clean_yellow(df):
    # re-parse timestamps defensively (CSV sometimes has odd values)
    df = (df
          .withColumn("pickup_ts",  F.to_timestamp(F.col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))
          .withColumn("dropoff_ts", F.to_timestamp(F.col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))
         )

    # basic data quality
    df = (df
          .filter(F.col("pickup_ts").isNotNull())
          .filter(F.col("dropoff_ts").isNotNull())
          .filter(F.col("trip_distance") >= 0)
          .filter(F.col("trip_distance") <= 200)
          .filter(F.col("total_amount").isNotNull())
          .filter(F.col("total_amount") <= 1000)
         )

    # derive partitions from the parsed timestamp
    df = (df
          .withColumn("year",  F.year("pickup_ts"))
          .withColumn("month", F.month("pickup_ts"))
         )

    # hard filter to the months you actually downloaded (Jan–Mar 2019)
    df = df.filter((F.col("year") == 2019) & (F.col("month").isin(1, 2, 3)))

    # keep original column names expected downstream
    df = (df
          .drop("tpep_pickup_datetime", "tpep_dropoff_datetime")
          .withColumnRenamed("pickup_ts",  "tpep_pickup_datetime")
          .withColumnRenamed("dropoff_ts", "tpep_dropoff_datetime")
         )
    return df

def daily_metrics(df):
    return (df.groupBy("year","month", F.to_date("tpep_pickup_datetime").alias("ds"))
            .agg(
                F.count("*").alias("trips"),
                F.sum("passenger_count").alias("passengers"),
                F.sum("trip_distance").alias("total_miles"),
                F.sum("total_amount").alias("revenue"),
                F.expr("percentile_approx(total_amount, 0.5)").alias("median_fare")
            ))
