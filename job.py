import sys, glob
from pyspark.sql import SparkSession
from schemas_2019 import yellow_2019_schema
from transforms import clean_yellow, daily_metrics

def main(input_glob, processed_path, curated_path):
    spark = (SparkSession.builder
             .appName("nyc-taxi-etl-local")
             .config("spark.sql.sources.partitionOverwriteMode","dynamic")
             .getOrCreate())

    files = sorted(glob.glob(input_glob))
    if not files:
        raise SystemExit(f"No files matched: {input_glob}")

    # Read raw CSVs with explicit schema
    raw = (spark.read
           .option("header", True)
           .schema(yellow_2019_schema)
           .csv(files))

    # Clean + add partitions
    cleaned = clean_yellow(raw)

    # Write processed trips (partitioned parquet)
    (cleaned
        .repartition("year","month")
        .write.mode("overwrite")
        .partitionBy("year","month")
        .parquet(processed_path))

    # Curated daily KPIs
    kpis = daily_metrics(cleaned)
    (kpis
        .repartition("year","month")
        .write.mode("overwrite")
        .partitionBy("year","month")
        .parquet(curated_path))

    spark.stop()

if __name__ == "__main__":
    # Example:
    # python job.py "yellow_tripdata_2019-0*.csv" out/processed/yellow out/curated/yellow_daily
    main(sys.argv[1], sys.argv[2], sys.argv[3])
