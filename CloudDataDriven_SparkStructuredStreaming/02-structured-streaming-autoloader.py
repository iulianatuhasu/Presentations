# Databricks notebook source
# MAGIC %md
# MAGIC JSON files -> Table

# COMMAND ----------

# CLEANUP
dbutils.fs.rm("dbfs:/mnt/demo/transactions_checkpoint", True)
spark.sql("DROP TABLE IF EXISTS bronze_transactions")

# COMMAND ----------

# Create the bronze_transactions streaming table
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_transactions (
    TransactionId STRING, 
    CustomerId STRING, 
    TransactionDate TIMESTAMP,
    TypeId STRING
) USING DELTA
""")

# COMMAND ----------

# MAGIC %run ./generate_and_load_json_files

# COMMAND ----------

# Read the json files from the source directory
streaming_df = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/transactions_checkpoint")
                .load(f"dbfs:{source_dir}")
                .withColumn("TransactionDate", F.col("TransactionDate").cast("timestamp"))
                .select("TransactionId","CustomerId","TransactionDate","TypeId"))

# Write the streaming data to the bronze_transactions table
query = (streaming_df.writeStream
         .format("delta")
         .option("checkpointLocation", "dbfs:/mnt/demo/transactions_checkpoint")
         .trigger(availableNow=True)
         .table("bronze_transactions")
         )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_transactions order by transactionid

# COMMAND ----------

# Load new data and list/display the new files
new_file_paths = load_new_data()
print("Newly generated files:")
for file_path in new_file_paths:
    print(file_path.split('/')[-1])
list_and_display_file_contents(new_file_paths)

# COMMAND ----------

# Delete initial and new files
delete_files([f"/dbfs{source_dir}/{file.name}" for file in initial_files])
delete_files(new_file_paths)
