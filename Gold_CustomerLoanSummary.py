# Databricks notebook source
# Load cleaned data from Silver layer
df_customers = spark.read.parquet("/mnt/boot-camp-project02/silver/customers")
df_loans = spark.read.parquet("/mnt/boot-camp-project02/silver/loans")
df_loan_payments = spark.read.parquet("/mnt/boot-camp-project02/silver/loan_payments")
df_accounts = spark.read.parquet("/mnt/boot-camp-project02/silver/accounts")
df_transactions = spark.read.parquet("/mnt/boot-camp-project02/silver/transactions")



# COMMAND ----------

from pyspark.sql.functions import sum as _sum

# Step 1: Join accounts with customers
df_customer_accounts = df_accounts.join(df_customers, on="customer_id", how="inner")

# Step 2: Group by customer_id and full_name, sum balance
df_total_balance = df_customer_accounts.groupBy("customer_id", "full_name") \
    .agg(_sum("balance").alias("total_balance"))

# Step 3: Join the aggregate back to include all columns from accounts and customers
df_enriched = df_customer_accounts.join(df_total_balance, on=["customer_id", "full_name"], how="left")

# Step 4: Save to Gold (Refined) container
df_enriched.write.format("parquet") \
    .mode("overwrite") \
    .save("/mnt/boot-camp-project02/gold/customer_account_balance")

# Step 5: Preview
display(df_enriched)


# COMMAND ----------

from pyspark.sql.functions import sum as _sum

# Join customers and loans
df_customer_loans = df_customers.join(df_loans, on="customer_id", how="inner")

# Aggregate: total loan amount per customer
df_total_loan_amount = df_customer_loans.groupBy("customer_id", "full_name") \
    .agg(_sum("loan_amount").alias("total_loan_amount"))

# Join back to include all details
df_customer_loans_enriched = df_customer_loans.join(
    df_total_loan_amount, on=["customer_id", "full_name"], how="left"
)

# Save to Gold
df_customer_loans_enriched.write.format("parquet") \
    .mode("overwrite") \
    .save("/mnt/boot-camp-project02/gold/customer_loan_summary_v2")

display(df_customer_loans_enriched)


# COMMAND ----------

from pyspark.sql.functions import when, sum as _sum, count as _count, col

# Join transactions with accounts
df_account_txn = df_transactions.join(df_accounts, on="account_id", how="inner")

# Create Deposit & Withdrawal columns
df_account_txn = df_account_txn \
    .withColumn("deposit_amount", when(col("transaction_type") == "Deposit", col("transaction_amount")).otherwise(0)) \
    .withColumn("withdrawal_amount", when(col("transaction_type") == "Withdrawal", col("transaction_amount")).otherwise(0))

# Aggregate per account
df_txn_agg = df_account_txn.groupBy("account_id") \
    .agg(
        _count("transaction_id").alias("total_transactions"),
        _sum("deposit_amount").alias("total_deposit"),
        _sum("withdrawal_amount").alias("total_withdrawal")
    ) \
    .withColumn("net_flow", col("total_deposit") - col("total_withdrawal"))

# Join back to accounts to include all original columns
df_account_summary = df_accounts.join(df_txn_agg, on="account_id", how="left")

# Save to Gold
df_account_summary.write.format("parquet") \
    .mode("overwrite") \
    .save("/mnt/boot-camp-project02/gold/account_transaction_summary_v2")

display(df_account_summary)
