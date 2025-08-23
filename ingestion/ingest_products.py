# Databricks notebook source
# Very sparse at the moment, but run some global configs
%run /Users/winsa.alva@gmail.com/tocaboca-case/ingestion/configuration

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Use spark read to parse the parquet file, inferring the schema
products_df = spark.read \
    .option("header", True) \
    .option("inferSchema", "true") \
    .parquet(f"{folder_path}/products")

# COMMAND ----------

final_df = products_df.withColumn("ingested_at", current_timestamp())

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS prod_bronze.tocaboca")

# Save the data frame as a delta table in unity catalog
final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("prod_bronze.tocaboca.products")
