# Databricks notebook source
# CLEANUP
dbutils.fs.rm("dbfs:/mnt/output/watermark_checkpoint", True)
spark.sql("DROP TABLE IF EXISTS bronze_events")
spark.sql("DROP TABLE IF EXISTS silver_watermarked_results")

# COMMAND ----------

# Create the source and dest tables

spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze_events (
        event_time TIMESTAMP,
        value STRING
    )
    USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_watermarked_results (
        start TIMESTAMP,
        end TIMESTAMP,
        value STRING,
        count LONG
    )
    USING DELTA
""")

# COMMAND ----------

from pyspark.sql.functions import window, col, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, TimestampType, StringType

# Read from the table as a streaming source
streaming_df = spark.readStream.table("bronze_events")

# Apply a watermark and a window aggregation

watermarked_df = streaming_df \
    .withWatermark("event_time", "4 minutes") \
    .groupBy(
        window(col("event_time"), "4 minutes"),
        col("value")
    ) \
    .count().alias("count") \
    .select("window.start", "window.end", "value", "count")

# Define the foreachBatch function to handle merging/updating
def merge_to_gold_table(batch_df, batch_id):
    batch_df.createOrReplaceTempView("batch_data")
    merge_query = """
    MERGE INTO silver_watermarked_results AS target
    USING (SELECT 
              start, 
              end, 
              value, 
              count 
           FROM batch_data) AS source
    ON target.start = source.start AND target.end = source.end AND target.value = source.value
    WHEN MATCHED THEN
      UPDATE SET
        target.count = source.count
    WHEN NOT MATCHED THEN
      INSERT (start, end, value, count)
      VALUES (source.start, source.end, source.value, source.count)
    """
    batch_df.sparkSession.sql(merge_query)

# Start the stream and write output using foreachBatch
query = watermarked_df \
    .writeStream \
    .outputMode("update") \
    .foreachBatch(merge_to_gold_table) \
    .option("checkpointLocation", "dbfs:/mnt/output/watermark_checkpoint") \
    .trigger(availableNow=True) \
    .start()


# COMMAND ----------

query.status

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, current_timestamp() from silver_watermarked_results order by start 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO bronze_events VALUES ("2023-07-18 14:00:00", "Letter A"); 
# MAGIC INSERT INTO bronze_events VALUES ("2023-07-18 14:05:00", "Letter A"); 
# MAGIC INSERT INTO bronze_events VALUES ("2023-07-18 14:06:00", "Letter A");
# MAGIC INSERT INTO bronze_events VALUES ("2023-07-18 14:09:00", "Letter A");
# MAGIC -- watermark = 0 when trigger starts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze_events VALUES ("2023-07-18 14:01:00", "Letter A");
# MAGIC -- watermark = 14:05 when trigger starts
