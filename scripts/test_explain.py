from pyspark.sql import SparkSession

# 1. Create Spark Session and disable auto broadcast to force a shuffle
spark = SparkSession.builder \
    .appName("SparkExplainDemo") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

# 2. Read two of your existing bronze parquet tables
df_orders = spark.read.parquet("data/bronze/orders")
df_customers = spark.read.parquet("data/bronze/customers")

# 3. Perform a join
df_joined = df_orders.join(df_customers, df_orders.customer_id == df_customers.customer_id)

# 4. Print the execution plans
df_joined.explain(True)

spark.stop()