import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp

def create_spark_session():
    return SparkSession.builder \
        .appName("EcommerceDataPlatform-SilverTransformation") \
        .getOrCreate()

def transform_silver():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    bronze_dir = "data/bronze"
    silver_dir = "data/silver"
    os.makedirs(silver_dir, exist_ok=True)

    print("Starting Silver Layer Transformation...")

    # 1. Transform Customers
    cust_path = os.path.join(bronze_dir, "customers")
    if os.path.exists(cust_path):
        df_cust = spark.read.parquet(cust_path)
        df_cust_clean = df_cust.dropDuplicates(["customer_id"]) \
                               .withColumn("signup_date", to_timestamp(col("signup_date"))) \
                               .withColumn("updated_at", to_timestamp(col("updated_at"))) \
                               .withColumn("processed_timestamp", current_timestamp())
        df_cust_clean.write.mode("overwrite").parquet(os.path.join(silver_dir, "customers"))
        print("-> Cleaned & transformed Customers into Silver.")

    # 2. Transform Products
    prod_path = os.path.join(bronze_dir, "products")
    if os.path.exists(prod_path):
        df_prod = spark.read.parquet(prod_path)
        df_prod_clean = df_prod.dropDuplicates(["product_id"]) \
                               .withColumn("unit_price", col("unit_price").cast("double")) \
                               .withColumn("processed_timestamp", current_timestamp())
        df_prod_clean.write.mode("overwrite").parquet(os.path.join(silver_dir, "products"))
        print("-> Cleaned & transformed Products into Silver.")

    # 3. Transform Orders
    orders_path = os.path.join(bronze_dir, "orders")
    if os.path.exists(orders_path):
        df_orders = spark.read.parquet(orders_path)
        df_orders_clean = df_orders.dropDuplicates(["order_id"]) \
                                   .withColumn("order_timestamp", to_timestamp(col("order_timestamp"))) \
                                   .withColumn("updated_at", to_timestamp(col("updated_at"))) \
                                   .withColumn("processed_timestamp", current_timestamp())
        df_orders_clean.write.mode("overwrite").parquet(os.path.join(silver_dir, "orders"))
        print("-> Cleaned & transformed Orders into Silver.")

    # 4. Transform Order Items
    items_path = os.path.join(bronze_dir, "order_items")
    if os.path.exists(items_path):
        df_items = spark.read.parquet(items_path)
        df_items_clean = df_items.dropDuplicates(["order_id", "product_id"]) \
                                 .withColumn("unit_price", col("unit_price").cast("double")) \
                                 .withColumn("discount", col("discount").cast("double")) \
                                 .withColumn("quantity", col("quantity").cast("integer")) \
                                 .withColumn("processed_timestamp", current_timestamp())
        df_items_clean.write.mode("overwrite").parquet(os.path.join(silver_dir, "order_items"))
        print("-> Cleaned & transformed Order Items into Silver.")

    # 5. Transform Payments
    pay_path = os.path.join(bronze_dir, "payments")
    if os.path.exists(pay_path):
        df_pay = spark.read.parquet(pay_path)
        df_pay_clean = df_pay.dropDuplicates(["payment_id"]) \
                             .withColumn("payment_amount", col("payment_amount").cast("double")) \
                             .withColumn("payment_timestamp", to_timestamp(col("payment_timestamp"))) \
                             .withColumn("processed_timestamp", current_timestamp())
        df_pay_clean.write.mode("overwrite").parquet(os.path.join(silver_dir, "payments"))
        print("-> Cleaned & transformed Payments into Silver.")

    # 6. Transform Events
    events_path = os.path.join(bronze_dir, "events")
    if os.path.exists(events_path):
        df_events = spark.read.parquet(events_path)
        df_events_clean = df_events.dropDuplicates(["event_id"]) \
                                   .withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
                                   .withColumn("processed_timestamp", current_timestamp())
        df_events_clean.write.mode("overwrite").parquet(os.path.join(silver_dir, "events"))
        print("-> Cleaned & transformed Events into Silver.")

    print("Silver Layer Transformation Completed Successfully!")
    spark.stop()

if __name__ == "__main__":
    transform_silver()
