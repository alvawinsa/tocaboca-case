def model(dbt, session):

    from pyspark.sql import functions as F

    dbt.config(materialized = "incremental")
    bronze_df = dbt.source("tocaboca","events")

    if dbt.is_incremental:

        # only new rows compared to latest record in current table
        max_from_this = f"select max(updated_at) from {dbt.this}"
        bronze_df = bronze_df.filter(bronze_df.updated_at >= session.sql(max_from_this).collect()[0][0])

    # Toggle env for dev vs prod
    environment = "dev"

    if environment == "dev":

        """THIS IS FOR LOCAL TEST AND DEVELOPMENT ENV - DEACTIVATE FOR PRODUCTION"""
        dbt.config(
            materialized="table",
            submission_method="all_purpose_cluster",
            cluster_id="0822-130740-uxbnq9bi",
            create_notebook=True,
            num_workers=2
        )

    elif environment == "prod":

        """THIS IS FOR PRODUCTION ENVIRONMENT - DEACTIVATE FOR THE DEV"""
        dbt.config(
            materialized="table",
            submission_method="job_cluster",
            job_cluster_config={
                "spark_version": "16.4.x-scala2.12",
                "node_type_id": "Standard_D4s_v4",
                "num_workers": 1
            },
        )
    else:
        raise Exception("No spark environment defined")

    # Flatten event_params (array of key/value structs) into columns
    flattened_df = (
        bronze_df
        .withColumn("param", F.explode_outer("event_params"))
        .withColumn("param_key", F.col("param.key"))
        .withColumn("param_value_string", F.col("param.value.string_value"))
        .withColumn("param_value_int", F.col("param.value.int_value"))
        .withColumn("param_value_double", F.col("param.value.double_value"))
    )

    # Pivot params back into columns based on unique combination of columns
    pivoted_df = (
        flattened_df
        .withColumn(
            "param_value",
            F.coalesce(
                F.col("param_value_string"),
                F.col("param_value_int").cast("string"),
                F.col("param_value_double").cast("string"),
            )
        )
        .groupBy(
            "event_date",
            "event_timestamp",
            "event_name",
            "device_id",
            "install_id",
            "device_category",
            "install_source",
            "ingested_at",
        )
        .pivot("param_key")
        .agg(F.first("param_value"))
    )

    # Cast columns into correct data types and drop duplicates
    final_df = (
        pivoted_df
        .withColumn("event_timestamp", (F.col("event_timestamp")/1e6).cast("timestamp"))
        .withColumn("price", F.col("price").cast("double") / 1e6)
        .withColumn("value", F.col("value").cast("double") / 1e6)
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("count", F.col("count").cast("int"))
        .withColumn("engaged_session_event", (F.col("engaged_session_event") == "1").cast("boolean"))
        .withColumn("firebase_conversion", (F.col("firebase_conversion") == "1").cast("boolean"))
        .withColumn("ga_session_id", F.col("ga_session_id").cast("long"))
        .withColumn("session_engaged", (F.col("session_engaged") == "1").cast("boolean"))
        .withColumn("subscription", (F.col("subscription") == "1").cast("boolean"))
        .withColumn("validated", (F.col("validated") == "1").cast("boolean"))
        .withColumn("updated_at", F.current_timestamp())
        .dropDuplicates(["event_timestamp", "event_name", "device_id", "install_id"])
    )

    return final_df