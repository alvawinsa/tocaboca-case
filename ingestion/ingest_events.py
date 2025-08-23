# Databricks notebook source
# Very sparse at the moment, but run some global configs
%run /Users/winsa.alva@gmail.com/tocaboca-case/ingestion/configuration

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Using spark read to read the parquet file and save to spark data frame
events_df = spark.read \
    .option("header", True) \
    .option("inferSchema", "true") \
    .parquet(f"{folder_path}/events")

# COMMAND ----------

# Add an ingested at column 
final_df = events_df.withColumn("ingested_at", current_timestamp())

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS prod_bronze.tocaboca")

# Write data frame as a delta table to unity catalog
final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("prod_bronze.tocaboca.events")
