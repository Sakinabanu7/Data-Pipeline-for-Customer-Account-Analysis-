from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("JoinAndAggregate").getOrCreate()

# 1. Read curated data
df_accounts = spark.read.parquet("gs://silver-data-pipeline-455700/accounts")
df_customers = spark.read.parquet("gs://silver-data-pipeline-455700/customers")

# 2. Join on customer_id
df_joined = df_customers.join(df_accounts, on="customer_id", how="inner")

# 3. Compute total balance per customer
df_total_balance = df_joined.groupBy(
    "customer_id", "first_name", "last_name"
).agg(
    sum("balance").alias("total_balance")
)

# 4. Write output to gold bucket
df_total_balance.write.mode("overwrite") \
    .parquet("gs://gold-data-pipeline-455700/customer_account_balance")

print("Aggregation complete. Output written to gold bucket.")
