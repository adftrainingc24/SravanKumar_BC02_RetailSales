# Databricks notebook source
from pyspark.sql.functions import col, when
from pyspark.sql import Row

# Step 1: Load existing Gold table
df_gold = spark.read.parquet("/mnt/boot-camp-project02/gold/customer_account_balance/")

# Step 2: Update an existing record (customer_id = 1, account_id = 88)
df_gold_updated = df_gold.withColumn(
    "balance",
    when((col("customer_id") == 1) & (col("account_id") == 88), 9999.99).otherwise(col("balance"))
)

# Step 3: Create a new record
new_record = spark.createDataFrame([
    Row(
        customer_id=999,
        full_name="Test User",
        account_id=9999,
        account_type="Checking",
        balance=1234.56,
        first_name="Test",
        last_name="User",
        address="999 Test Lane",
        city="Toronto",
        state="ON",
        zip="M5T1A1",
        total_balance=1234.56
    )
])

# Step 4: Append new record
df_final = df_gold_updated.unionByName(new_record)

# Step 5: Overwrite the Gold table
# Overwrite the parquet file
df_final.write.mode("overwrite").parquet("/mnt/boot-camp-project02/gold/customer_account_balance/")

# 🟢 Re-read the Parquet file to avoid stale metadata issues
df_reloaded = spark.read.parquet("/mnt/boot-camp-project02/gold/customer_account_balance/")
df_reloaded.filter(col("customer_id") == 999).show()


# COMMAND ----------

