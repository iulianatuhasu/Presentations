# Databricks notebook source
# MAGIC %md
# MAGIC # Stream-Static Joins

# COMMAND ----------

# CLEANUP
dbutils.fs.rm("dbfs:/mnt/data/checkpoints/stream_static_join", True)
spark.sql("DROP TABLE IF EXISTS transactions_type")
spark.sql("DROP TABLE IF EXISTS bronze_transactions")
spark.sql("DROP TABLE IF EXISTS silver_transactions")

# COMMAND ----------

# Create the transactions_type static table schema and insert data - will be used as static 
spark.sql("""
CREATE TABLE IF NOT EXISTS transactions_type (
    TypeId STRING, 
    TypeName STRING
) USING DELTA
""")

# Insert initial data into transactions_type static table
spark.sql("INSERT INTO transactions_type VALUES ('1', 'Card')")
spark.sql("INSERT INTO transactions_type VALUES ('2', 'Cash')")
#spark.sql("INSERT INTO transactions_type VALUES ('4', 'Bank Statement')")


# Create the bronze_transactions streaming table schema - will be used as streaming
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_transactions (
    TransactionId STRING, 
    CustomerId STRING, 
    TransactionDate TIMESTAMP,
    TypeId STRING
) USING DELTA
""")

# Insert initial data into bronze_transactions table 
spark.sql("INSERT INTO bronze_transactions VALUES ('T001', '1', '2023-07-20 10:00:00', '1')")
spark.sql("INSERT INTO bronze_transactions VALUES ('T002', '2', '2023-07-21 11:00:00', '2')")
spark.sql("INSERT INTO bronze_transactions VALUES ('T003', '3', '2023-07-22 12:00:00', '4')")

# Create the silver_transactions table - to insert the output of the joined dataframes
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_transactions (
    TransactionId STRING, 
    CustomerId STRING, 
    TransactionDate TIMESTAMP,
    TypeId STRING,
    TypeName STRING
) USING DELTA
""")

# COMMAND ----------

# Read from bronze_transactions table as a streaming DataFrame
bronze_stream = (
    spark.readStream
    .format("delta")
    .table("bronze_transactions")
)

transactions_type_df = spark.read.table("transactions_type")

# Join the streaming dataframe with the static transactions_type table
joined_df = bronze_stream.join(
    transactions_type_df,
    bronze_stream.TypeId == transactions_type_df.TypeId,
    "inner"
).select(
    "TransactionId",
    "CustomerId",
    "TransactionDate",
    "bronze_transactions.TypeId",
    "TypeName"
)

# Write the joined stream to the output table 
query = (
    joined_df.writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", "/mnt/data/checkpoints/stream_static_join")
    .trigger(availableNow=True)
    .table("silver_transactions")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_transactions

# COMMAND ----------

spark.sql("INSERT INTO transactions_type VALUES ('4', 'Bank Statement')")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from transactions_type

# COMMAND ----------

# MAGIC %md
# MAGIC # Stream-Stream Joins

# COMMAND ----------

# CLEANUP
dbutils.fs.rm("dbfs:/mnt/data/checkpoints/stream_stream_join", True)
spark.sql("DROP TABLE IF EXISTS bronze_transactions")
spark.sql("DROP TABLE IF EXISTS bronze_customers")
spark.sql("DROP TABLE IF EXISTS silver_transactions")

# COMMAND ----------

# Create the bronze_transactions streaming table schema
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_transactions (
    TransactionId STRING, 
    CustomerId STRING, 
    TransactionDate TIMESTAMP,
    TypeId STRING
) USING DELTA
""")

# Insert initial data into bronze_transactions table
spark.sql("INSERT INTO bronze_transactions VALUES ('T001', '1', '2023-07-20 10:00:00', '1')")
spark.sql("INSERT INTO bronze_transactions VALUES ('T002', '2', '2023-07-21 11:00:00', '2')")
spark.sql("INSERT INTO bronze_transactions VALUES ('T003', '4', '2023-07-22 12:00:00', '4')")



# Create the bronze_customers streaming table schema
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_customers ( 
    CustomerId STRING, 
    EmailAddress STRING
) USING DELTA
""")

# Insert initial data into bronze_customers table
spark.sql("INSERT INTO bronze_customers VALUES ('1', 'test@yahoo.com')")
spark.sql("INSERT INTO bronze_customers VALUES ('2', 'test@gmail.com')")
spark.sql("INSERT INTO bronze_customers VALUES ('3', 'test@abc.com')")



# COMMAND ----------

# Create streaming DataFrame for transactions
transactions_stream = spark.readStream.format("delta").table("bronze_transactions")

# Create streaming DataFrame for customers
customers_stream = spark.readStream.format("delta").table("bronze_customers")

# Perform stream-stream join
joined_stream = transactions_stream.join(
    customers_stream,
    transactions_stream.CustomerId == customers_stream.CustomerId,
    "inner"
).select(
    transactions_stream.TransactionId,
    transactions_stream.CustomerId,
    transactions_stream.TransactionDate,
    transactions_stream.TypeId,
    customers_stream.EmailAddress
)

# Write the joined stream to the output table
query = (
    joined_stream.writeStream
    .outputMode("append")
    .format("delta")
    .trigger(availableNow=True)
    .option("checkpointLocation", "/mnt/data/checkpoints/stream_stream_join")
    .table("silver_transactions")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_transactions

# COMMAND ----------

spark.sql("INSERT INTO bronze_customers VALUES ('4', 'abc@abc.com')")
