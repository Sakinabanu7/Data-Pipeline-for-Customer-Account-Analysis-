from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("DataprocCleaning").getOrCreate()

# ---------------- ACCOUNTS ----------------
accounts_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True)
])

df_accounts = spark.read.schema(accounts_schema).option("header", "true") \
    .csv("gs://bronze-data-pipeline-455700/accounts.csv")

df_accounts_cleaned = df_accounts.dropna(subset=["account_id", "customer_id", "balance"]) \
                                 .filter(col("balance") >= 0)

df_accounts_cleaned.write.mode("append") \
    .parquet("gs://silver-data-pipeline-455700/accounts")


# ---------------- CUSTOMERS ----------------
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True)
])

df_customers = spark.read.schema(customers_schema).option("header", "true") \
    .csv("gs://bronze-data-pipeline-455700/customers.csv")

df_customers_cleaned = df_customers.dropna(subset=["customer_id", "first_name", "last_name"])

df_customers_cleaned.write.mode("append") \
    .parquet("gs://silver-data-pipeline-455700/customers")


# ---------------- LOAN PAYMENTS ----------------
loan_payments_schema = StructType([
    StructField("payment_id", StringType(), False),
    StructField("loan_id", StringType(), False),
    StructField("payment_date", StringType(), True),
    StructField("payment_amount", DoubleType(), True)
])

df_loan_payments = spark.read.schema(loan_payments_schema).option("header", "true") \
    .csv("gs://bronze-data-pipeline-455700/loan_payments.csv")

df_loan_cleaned = df_loan_payments.dropna(subset=["payment_id", "loan_id", "payment_amount"]) \
                                  .filter(col("payment_amount") > 0)

df_loan_cleaned.write.mode("append") \
    .parquet("gs://silver-data-pipeline-455700/loan_payments")


# ---------------- LOANS ----------------
loans_schema = StructType([
    StructField("loan_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("loan_amount", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("loan_term", IntegerType(), True)
])

df_loans = spark.read.schema(loans_schema).option("header", "true") \
    .csv("gs://bronze-data-pipeline-455700/loans.csv")

df_loans_cleaned = df_loans.dropna(subset=["loan_id", "customer_id", "loan_amount"]) \
                           .filter((col("loan_amount") > 0) & (col("interest_rate") > 0))

df_loans_cleaned.write.mode("append") \
    .parquet("gs://silver-data-pipeline-455700/loans")


# ---------------- TRANSACTIONS ----------------
transactions_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("transaction_date", StringType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True)
])

df_transactions = spark.read.schema(transactions_schema).option("header", "true") \
    .csv("gs://bronze-data-pipeline-455700/transactions.csv")

df_transactions_cleaned = df_transactions.dropna(subset=["transaction_id", "account_id", "transaction_amount"]) \
                                         .filter(col("transaction_amount") > 0)

df_transactions_cleaned.write.mode("append") \
    .parquet("gs://silver-data-pipeline-455700/transactions")

print(" All five datasets cleaned and written to Silver bucket.")
