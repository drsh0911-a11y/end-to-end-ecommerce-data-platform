import os
from pyspark.sql import SparkSession

def verify_gold():
    spark = SparkSession.builder \
        .appName("EcommerceDataPlatform-GoldVerification") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    gold_dir = "data/gold"
    datasets = ["fct_daily_sales", "fct_customer_metrics", "fct_product_performance"]

    print("--- Gold Layer Verification ---")
    for ds in datasets:
        ds_path = os.path.join(gold_dir, ds)
        if os.path.exists(ds_path):
            df = spark.read.parquet(ds_path)
            print(f"\n[Dataset: {ds}]")
            print(f"Count: {df.count()} rows")
            df.printSchema()
            df.show(5, truncate=False)
        else:
            print(f"\n[Dataset: {ds}] NOT FOUND at {ds_path}")

    spark.stop()

if __name__ == "__main__":
    verify_gold()
