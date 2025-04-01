# Databricks notebook source
df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("/mnt/boot-camp-project02/bronze/loans.csv")

display(df)


# COMMAND ----------

import os

bronze_path = "/mnt/boot-camp-project02/bronze/"

files = dbutils.fs.ls(bronze_path)

dataframes = {}

for file in files:
    file_name = file.name
    name = file_name.replace(".csv", "")  # e.g., "accounts"
    
    df = spark.read.csv(f"{bronze_path}{file_name}", header=True, inferSchema=True)
    dataframes[name] = df

# Example: display 'customers' DataFrame
display(dataframes['accounts'])


# COMMAND ----------

from pyspark.sql.functions import col

# Step 1: Read accounts.csv directly from Bronze
df_accounts = spark.read.csv(
    "/mnt/boot-camp-project02/bronze/accounts.csv", 
    header=True, 
    inferSchema=True
)

# Step 2: Verify schema
df_accounts.printSchema()

# Step 3: Clean the data
df_accounts_cleaned = df_accounts.dropna(subset=["account_id", "customer_id"])
df_accounts_cleaned = df_accounts_cleaned.dropDuplicates(["account_id"])
df_accounts_cleaned = df_accounts_cleaned.withColumn("balance", col("balance").cast("double"))

# Step 4: Save to Silver layer
df_accounts_cleaned.write.format("parquet") \
    .mode("overwrite") \
    .save("/mnt/boot-camp-project02/silver/accounts")

# Step 5: Display
display(df_accounts_cleaned)


# COMMAND ----------

from pyspark.sql.functions import col, concat_ws

df_customers = spark.read.csv(
    "/mnt/boot-camp-project02/bronze/customers.csv",
    header=True,
    inferSchema=True
)

# Drop nulls in key columns
df_customers_cleaned = df_customers.dropna(subset=["customer_id"])

# Drop duplicates
df_customers_cleaned = df_customers_cleaned.dropDuplicates(["customer_id"])

# Add full_name column
df_customers_cleaned = df_customers_cleaned.withColumn(
    "full_name", concat_ws(" ", col("first_name"), col("last_name"))
)

# Write to Silver layer
df_customers_cleaned.write.format("parquet") \
    .mode("overwrite") \
    .save("/mnt/boot-camp-project02/silver/customers")

# Optional: View
display(df_customers_cleaned)


# COMMAND ----------

from pyspark.sql.functions import col

df_loans = spark.read.csv(
    "/mnt/boot-camp-project02/bronze/loans.csv",
    header=True,
    inferSchema=True
)

# Step 1: Drop rows with missing loan_id or customer_id
df_loans_cleaned = df_loans.dropna(subset=["loan_id", "customer_id"])

# Step 2: Drop duplicates based on loan_id
df_loans_cleaned = df_loans_cleaned.dropDuplicates(["loan_id"])

# Step 3: Cast numeric columns
df_loans_cleaned = df_loans_cleaned \
    .withColumn("loan_amount", col("loan_amount").cast("double")) \
    .withColumn("interest_rate", col("interest_rate").cast("double")) \
    .withColumn("loan_term", col("loan_term").cast("int"))

# Step 4: Save to Silver layer
df_loans_cleaned.write.format("parquet") \
    .mode("overwrite") \
    .save("/mnt/boot-camp-project02/silver/loans")

# Step 5: Preview
display(df_loans_cleaned)


# COMMAND ----------

from pyspark.sql.functions import col

df_loan_payments = spark.read.csv(
    "/mnt/boot-camp-project02/bronze/loan_payments.csv",
    header=True,
    inferSchema=True
)

# Step 1: Drop rows with missing payment_id or loan_id
df_loan_payments_cleaned = df_loan_payments.dropna(subset=["payment_id", "loan_id"])

# Step 2: Drop duplicates based on payment_id
df_loan_payments_cleaned = df_loan_payments_cleaned.dropDuplicates(["payment_id"])

# Step 3: Cast numeric and date columns
df_loan_payments_cleaned = df_loan_payments_cleaned \
    .withColumn("payment_amount", col("payment_amount").cast("double")) \
    .withColumn("payment_date", col("payment_date").cast("date"))

# Step 4: Write to Silver layer
df_loan_payments_cleaned.write.format("parquet") \
    .mode("overwrite") \
    .save("/mnt/boot-camp-project02/silver/loan_payments")

# Step 5: Display sample
display(df_loan_payments_cleaned)


# COMMAND ----------

from pyspark.sql.functions import col, abs as abs_val

df_transactions = spark.read.csv(
    "/mnt/boot-camp-project02/bronze/transactions.csv",
    header=True,
    inferSchema=True
)

# Step 1: Drop rows with null key fields
df_transactions_cleaned = df_transactions.dropna(subset=["transaction_id", "account_id"])

# Step 2: Drop duplicates based on transaction_id
df_transactions_cleaned = df_transactions_cleaned.dropDuplicates(["transaction_id"])

# Step 3: Cast amount and date
df_transactions_cleaned = df_transactions_cleaned \
    .withColumn("transaction_amount", col("transaction_amount").cast("double")) \
    .withColumn("transaction_date", col("transaction_date").cast("date"))

# Step 4: Add absolute amount column
df_transactions_cleaned = df_transactions_cleaned.withColumn(
    "transaction_amount_abs", abs_val(col("transaction_amount"))
)

# Step 5: Save to Silver layer
df_transactions_cleaned.write.format("parquet") \
    .mode("overwrite") \
    .save("/mnt/boot-camp-project02/silver/transactions")

# Step 6: Display
display(df_transactions_cleaned)


# COMMAND ----------

display(df)