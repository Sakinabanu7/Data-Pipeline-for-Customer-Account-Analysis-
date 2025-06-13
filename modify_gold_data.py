from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Start Spark
spark = SparkSession.builder.appName("ModifyGoldParquet").getOrCreate()

# Read existing gold data
df_gold = spark.read.parquet("gs://gold-data-pipeline-455700/customer_account_balance/")

# 1️⃣ Modify an existing customer (change balance to 99999.99)
# Pick any existing customer_id
existing_customer_id = df_gold.select("customer_id").limit(1).collect()[0][0]

update_row = df_gold.filter(df_gold.customer_id == existing_customer_id) \
                    .withColumn("total_balance", lit(99999.99))

# 2️⃣ Add a new customer record
new_data = [("NEWCUST001", "Fatima", "Ali", "55 Cedar Rd", "Toronto", "ON", "M4B1B3", 88888.88)]
new_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("total_balance", DoubleType(), True)
])
new_row = spark.createDataFrame(new_data, new_schema)

# Combine updated + new
df_updated = update_row.union(new_row)

# Overwrite Parquet in Gold bucket
df_updated.write.mode("overwrite").parquet("gs://gold-data-pipeline-455700/customer_account_balance/")

print("✅ Updated + new record written to gold bucket.")
