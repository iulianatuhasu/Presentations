# Databricks notebook source
# CLEANUP

dbutils.fs.rm("/mnt/data/checkpoints/bronze_to_silver_append", recurse=True)
spark.sql("DROP TABLE IF EXISTS bronze_transactions")
spark.sql("DROP TABLE IF EXISTS silver_transactions")

# COMMAND ----------

# Create bronze table
spark.sql(f"CREATE TABLE IF NOT EXISTS bronze_transactions (TransactionId STRING, id STRING, TransactionDate TIMESTAMP)")

# Insert initial data into bronze table
spark.sql("INSERT INTO bronze_transactions VALUES ('T001', 'C001', '2024-07-10 10:00:00')")
spark.sql("INSERT INTO bronze_transactions VALUES ('T002', 'C002', '2024-07-10 11:00:00')")

# Create silver table
spark.sql(f"CREATE TABLE IF NOT EXISTS silver_transactions (TransactionId STRING, CustomerId STRING, TransactionDate TIMESTAMP)")

# Read from bronze table
bronze_stream = (
    spark.readStream
    .format("delta")
    .table("bronze_transactions")
)

# Transformation: Rename id to CustomerId
bronze_transformed = bronze_stream.withColumnRenamed("id", "CustomerId")

# COMMAND ----------

silver_stream_append = (
    bronze_transformed
    .writeStream
    .format("delta")
    .outputMode("append")
    #.trigger(availableNow=True)
    .option("checkpointLocation", "/mnt/data/checkpoints/bronze_to_silver_append")
    .table("silver_transactions")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_transactions

# COMMAND ----------

spark.sql("INSERT INTO bronze_transactions VALUES ('T003', 'C003', '2024-07-11 11:00:00')")
spark.sql("INSERT INTO bronze_transactions VALUES ('T004', 'C004', '2024-07-11 11:00:00')")

# COMMAND ----------

spark.sql("INSERT INTO bronze_transactions VALUES ('T005', 'C005', '2024-07-11 11:00:00')")
spark.sql("INSERT INTO bronze_transactions VALUES ('T006', 'C006', '2024-07-11 11:00:00')")

# COMMAND ----------

silver_stream_append.stop()
