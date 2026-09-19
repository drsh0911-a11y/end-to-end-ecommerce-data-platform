import os
from pyspark.sql import SparkSession

def verify_silver():
    spark = SparkSession.builder \
        .appName("EcommerceDataPlatform-SilverVerification") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    silver_dir = "data/silver"
    datasets = ["customers", "products", "orders", "order_items", "payments", "events"]

    print("--- Silver Layer Verification ---")
    for ds in datasets:
        ds_path = os.path.join(silver_dir, ds)
        if os.path.exists(ds_path):
            df = spark.read.parquet(ds_path)
            print(f"\n[Dataset: {ds}]")
            print(f"Count: {df.count()} rows")
            df.printSchema()
        else:
            print(f"\n[Dataset: {ds}] NOT FOUND at {ds_path}")

    spark.stop()

if __name__ == "__main__":
    verify_silver()
