# Databricks notebook source
# MAGIC %run /Users/winsa.alva@gmail.com/tocaboca-case/ingestion/includes/configuration

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Use spark read to parse the parquet file, inferring the schema
products_df = (
    spark.read.option("header", True)
    .option("inferSchema", "true")
    .parquet(f"{FOLDER_PATH}/products")
)

# Check that there's any data
if products_df.count() == 0:
    raise ValueError("No data found in source!")

print(f"Read {products_df.count()} records from {FOLDER_PATH}/events")

# COMMAND ----------

final_df = products_df.withColumn("ingested_at", current_timestamp())

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")

# Save the data frame as a delta table in unity catalog
final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{BRONZE_SCHEMA}.{PRODUCTS_TABLE}")
    
print("Products ingestion completed.")
