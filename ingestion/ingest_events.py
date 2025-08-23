# Databricks notebook source
# MAGIC %run /Users/winsa.alva@gmail.com/tocaboca-case/ingestion/includes/configuration

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Using spark read to read the parquet file and save to spark data frame
events_df = spark.read \
    .option("header", True) \
    .option("inferSchema", "true") \
    .parquet(f"{FOLDER_PATH}/events")
print(f"Read {events_df.count()} records from {FOLDER_PATH}/events")

# COMMAND ----------

# Add an ingested at column 
final_df = events_df.withColumn("ingested_at", current_timestamp())

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")

# Write data frame as a delta table to unity catalog
final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{BRONZE_SCHEMA}.{EVENTS_TABLE}")
print("Events ingestion completed.")
