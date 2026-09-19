import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

def create_spark_session():
    return SparkSession.builder \
        .appName("EcommerceDataPlatform-BronzeIngestion") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

def ingest_bronze():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    source_dir = "data/source"
    bronze_dir = "data/bronze"
    os.makedirs(bronze_dir, exist_ok=True)

    print("Starting Bronze Layer Ingestion...")

    # 1. Ingest Customers (CSV)
    customers_path = os.path.join(source_dir, "customers.csv")
    if os.path.exists(customers_path):
        df_customers = spark.read.option("header", "true").option("inferSchema", "true").csv(customers_path)
        df_customers = df_customers.withColumn("ingestion_timestamp", current_timestamp()) \
                                   .withColumn("source_file", lit("customers.csv"))
        df_customers.write.mode("overwrite").parquet(os.path.join(bronze_dir, "customers"))
        print("-> Customers ingested successfully into Bronze.")

    # 2. Ingest Products (CSV)
    products_path = os.path.join(source_dir, "products.csv")
    if os.path.exists(products_path):
        df_products = spark.read.option("header", "true").option("inferSchema", "true").csv(products_path)
        df_products = df_products.withColumn("ingestion_timestamp", current_timestamp()) \
                                   .withColumn("source_file", lit("products.csv"))
        df_products.write.mode("overwrite").parquet(os.path.join(bronze_dir, "products"))
        print("-> Products ingested successfully into Bronze.")

    # 3. Ingest Orders (JSON)
    orders_path = os.path.join(source_dir, "orders.json")
    if os.path.exists(orders_path):
        df_orders = spark.read.option("multiline", "true").json(orders_path)
        df_orders = df_orders.withColumn("ingestion_timestamp", current_timestamp()) \
                             .withColumn("source_file", lit("orders.json"))
        df_orders.write.mode("overwrite").parquet(os.path.join(bronze_dir, "orders"))
        print("-> Orders ingested successfully into Bronze.")

    # 4. Ingest Order Items (JSON)
    order_items_path = os.path.join(source_dir, "order_items.json")
    if os.path.exists(order_items_path):
        df_order_items = spark.read.option("multiline", "true").json(order_items_path)
        df_order_items = df_order_items.withColumn("ingestion_timestamp", current_timestamp()) \
                                     .withColumn("source_file", lit("order_items.json"))
        df_order_items.write.mode("overwrite").parquet(os.path.join(bronze_dir, "order_items"))
        print("-> Order Items ingested successfully into Bronze.")

    # 5. Ingest Payments (JSON)
    payments_path = os.path.join(source_dir, "payments.json")
    if os.path.exists(payments_path):
        df_payments = spark.read.option("multiline", "true").json(payments_path)
        df_payments = df_payments.withColumn("ingestion_timestamp", current_timestamp()) \
                             .withColumn("source_file", lit("payments.json"))
        df_payments.write.mode("overwrite").parquet(os.path.join(bronze_dir, "payments"))
        print("-> Payments ingested successfully into Bronze.")

    # 6. Ingest Events (JSON)
    events_path = os.path.join(source_dir, "events.json")
    if os.path.exists(events_path):
        df_events = spark.read.option("multiline", "true").json(events_path)
        df_events = df_events.withColumn("ingestion_timestamp", current_timestamp()) \
                             .withColumn("source_file", lit("events.json"))
        df_events.write.mode("overwrite").parquet(os.path.join(bronze_dir, "events"))
        print("-> Events ingested successfully into Bronze.")

    print("Bronze Layer Ingestion Completed Successfully!")
    
    # Keep the Spark UI alive for 5 minutes so you can inspect it at http://localhost:4040
    print("Holding Spark UI open for 5 minutes... Press Ctrl+C to exit early.")
    time.sleep(300)

    spark.stop()

if __name__ == "__main__":
    ingest_bronze()