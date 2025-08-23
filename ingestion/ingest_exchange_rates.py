# Databricks notebook source
# Very sparse at the moment, but run some global configs
%run /Users/winsa.alva@gmail.com/tocaboca-case/ingestion/configuration

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
import pyarrow.parquet as pq
import pyarrow as pa

# COMMAND ----------

# Since we're using pyarrow, need to read from dbfs
table = pq.read_table(f"/dbfs/{folder_path}/exchange_rates")

# COMMAND ----------

# Spark can't handle more than decimal with precision of 38, so we first need to read it using pyarrow, then convert the usd_per_currency to a string 
table = table.set_column(
    table.schema.get_field_index("usd_per_currency"),
    "usd_per_currency",
    table["usd_per_currency"].cast(pa.string())
)

# COMMAND ----------

pq.write_table(
    table,
    "/dbfs/mnt/analyticsaw/tocaboca/exchange_rates_fixed"
)

# COMMAND ----------

converted_df = spark.read.parquet("/mnt/analyticsaw/tocaboca/exchange_rates_fixed")

# COMMAND ----------

# Add an ingested at timestamp
final_df = converted_df.withColumn("ingested_at", current_timestamp())

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS prod_bronze.tocaboca")

# Save the data frame as a delta table in unity catalog
final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("prod_bronze.tocaboca.exchange_rates")
