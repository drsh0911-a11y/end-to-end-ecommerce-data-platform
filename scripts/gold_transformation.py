import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, countDistinct, avg, max, min, to_date, round, current_timestamp

def create_spark_session():
    return SparkSession.builder \
        .appName("EcommerceDataPlatform-GoldTransformation") \
        .getOrCreate()

def transform_gold():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    silver_dir = "data/silver"
    gold_dir = "data/gold"
    os.makedirs(gold_dir, exist_ok=True)

    print("Starting Gold Layer Transformation...")

    # Load Silver tables
    df_orders = spark.read.parquet(os.path.join(silver_dir, "orders"))
    df_items = spark.read.parquet(os.path.join(silver_dir, "order_items"))
    df_cust = spark.read.parquet(os.path.join(silver_dir, "customers"))
    df_prod = spark.read.parquet(os.path.join(silver_dir, "products"))

    # Calculate item-level total price: quantity * unit_price * (1 - discount)
    df_item_sales = df_items.withColumn(
        "item_revenue", 
        round(col("quantity") * col("unit_price") * (1 - col("discount")), 2)
    )

    # 1. Daily Sales Performance (fct_daily_sales)
    df_order_sales = df_orders.filter(col("order_status") != "CANCELLED") \
        .join(df_item_sales, "order_id", "inner") \
        .withColumn("order_date", to_date(col("order_timestamp")))

    df_daily_sales = df_order_sales.groupBy("order_date", "country") \
        .agg(
            round(sum("item_revenue"), 2).alias("total_revenue"),
            countDistinct("order_id").alias("total_orders"),
            sum("quantity").alias("total_units_sold"),
            round(avg("item_revenue"), 2).alias("avg_item_value")
        ) \
        .withColumn("processed_timestamp", current_timestamp())

    df_daily_sales.write.mode("overwrite").parquet(os.path.join(gold_dir, "fct_daily_sales"))
    print("-> Gold: Created fct_daily_sales")

    # 2. Customer Lifetime Value & Profile Metrics (fct_customer_metrics)
    df_cust_orders = df_orders.filter(col("order_status") != "CANCELLED") \
        .join(df_item_sales, "order_id", "inner")

    df_cust_metrics = df_cust_orders.groupBy("customer_id") \
        .agg(
            round(sum("item_revenue"), 2).alias("lifetime_value"),
            countDistinct("order_id").alias("total_orders"),
            min("order_timestamp").alias("first_order_timestamp"),
            max("order_timestamp").alias("latest_order_timestamp")
        )

    df_fct_customer = df_cust.join(df_cust_metrics, "customer_id", "left") \
        .withColumn("processed_timestamp", current_timestamp())

    df_fct_customer.write.mode("overwrite").parquet(os.path.join(gold_dir, "fct_customer_metrics"))
    print("-> Gold: Created fct_customer_metrics")

    # 3. Product Performance Metrics (fct_product_performance)
    df_prod_metrics = df_item_sales.groupBy("product_id") \
        .agg(
            round(sum("item_revenue"), 2).alias("total_product_revenue"),
            sum("quantity").alias("total_units_sold"),
            round(avg("discount"), 4).alias("avg_discount_applied")
        )

    df_fct_product = df_prod.join(df_prod_metrics, "product_id", "left") \
        .withColumn("processed_timestamp", current_timestamp())

    df_fct_product.write.mode("overwrite").parquet(os.path.join(gold_dir, "fct_product_performance"))
    print("-> Gold: Created fct_product_performance")

    print("\nGold Layer Transformation Completed Successfully!")
    spark.stop()

if __name__ == "__main__":
    transform_gold()
