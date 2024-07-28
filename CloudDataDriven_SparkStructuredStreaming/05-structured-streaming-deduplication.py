# Databricks notebook source
# CLEANUP
dbutils.fs.rm("dbfs:/mnt/data/checkpoints/silver_transactions", True)
spark.sql("DROP TABLE IF EXISTS bronze_transactions")
spark.sql("DROP TABLE IF EXISTS silver_transactions")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


# Create the bronze_transactions table schema
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_transactions (
    TransactionId STRING, 
    id STRING, 
    TransactionDate TIMESTAMP,
    TypeId STRING
) USING DELTA
""")

# Insert initial data into bronze_transactions table
spark.sql("INSERT INTO bronze_transactions VALUES ('T001', '1', '2023-07-22 10:00:00', '1')")
spark.sql("INSERT INTO bronze_transactions VALUES ('T002', '2', '2023-07-22 11:00:00', '2')")
spark.sql("INSERT INTO bronze_transactions VALUES ('T003', '3', '2023-07-22 12:00:00', '3')")
spark.sql("INSERT INTO bronze_transactions VALUES ('T001', '1', '2023-07-22 10:00:00', '6')")  # Duplicate
spark.sql("INSERT INTO bronze_transactions VALUES ('T002', '2', '2023-07-22 11:00:00', '6')")  # Duplicate


# Create the silver_transactions table schema
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_transactions (
    TransactionId STRING, 
    CustomerId STRING, 
    TransactionDate TIMESTAMP,
    TypeId STRING
) USING DELTA
""")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_transactions

# COMMAND ----------

# Read from bronze_transactions table as a streaming DataFrame
bronze_stream = (
    spark.readStream
    .format("delta")
    .table("bronze_transactions")
)

# Transformation: Rename id to CustomerId
bronze_transformed = bronze_stream.withColumnRenamed("id", "CustomerId")

# Define dedup streaming df
deduped_df = (bronze_transformed
                   .withWatermark("TransactionDate", "30 seconds")
                   .dropDuplicates(["TransactionId", "TransactionDate"]))

# Define function to upsert data
def upsert_data(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("transactions_microbatch")
    
    sql_query = """
      MERGE INTO silver_transactions a
      USING transactions_microbatch b
      ON a.TransactionId=b.TransactionId and a.TransactionDate=b.TransactionDate
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(sql_query)

# Write to output table
query = (deduped_df.writeStream
                   .foreachBatch(upsert_data)
                   .option("checkpointLocation", "dbfs:/mnt/data/checkpoints/silver_transactions")
                   .trigger(availableNow=True)
                   .start()
                   )


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_transactions order by TransactionId

# COMMAND ----------

# MAGIC %md
# MAGIC Max event time seen = 12:00:00, threshold = 30 seconds =>  watermark = 11:59:30

# COMMAND ----------

# Insert additional data to simulate late arrivals
spark.sql("INSERT INTO bronze_transactions VALUES ('T004', '4', '2023-07-22 11:59:00', '1')")  # Late data before watermark
spark.sql("INSERT INTO bronze_transactions VALUES ('T005', '5', '2023-07-22 11:59:50', '3')")  # Late data within watermark
spark.sql("INSERT INTO bronze_transactions VALUES ('T006', '6', '2023-07-22 12:01:00', '3')")  # On-time data
