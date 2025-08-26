def model(dbt, session):

    from pyspark.sql import functions as F

    dbt.config(materialized = "incremental")
    bronze_df = dbt.source("tocaboca","events")

    if dbt.is_incremental:

        # Only new rows compared to latest record in current table
        max_from_this = f"select max(ingested_at) from {dbt.this}"
        bronze_df = bronze_df.filter(bronze_df.ingested_at >= session.sql(max_from_this).collect()[0][0])

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

    # Cast columns into correct data types, rename columns, drop duplicates and add synthetic key for uniqueness
    # Normally I'd split out some of these actions... but for sake of efficiency, it's all done in the final df.
    final_df = (
        pivoted_df
        .withColumnRenamed("currency", "currency_code")
        .withColumnRenamed("firebase_event_origin", "event_origin")
        .withColumnRenamed("firebase_screen_class", "screen_class")
        .withColumnRenamed("firebase_screen_id", "screen_id")
        .withColumnRenamed("device_id", "device_id_raw")
        .withColumn("event_timestamp", (F.col("event_timestamp")/1e6).cast("timestamp"))
        .withColumn("price", F.col("price").cast("double") / 1e6)
        .withColumn("value", F.col("value").cast("double") / 1e6)
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("count", F.col("count").cast("int"))
        .withColumn("is_engaged_session_event", (F.col("engaged_session_event") == "1").cast("boolean"))
        .withColumn("is_conversion_event", (F.col("firebase_conversion") == "1").cast("boolean"))
        .withColumn("ga_session_id", F.col("ga_session_id").cast("long"))
        .withColumn("is_session_engaged", (F.col("session_engaged") == "1").cast("boolean"))
        .withColumn("has_subscription", (F.col("subscription") == "1").cast("boolean"))
        .withColumn("validated", (F.col("validated") == "1").cast("boolean"))
        .withColumn("product_name", F.lower(F.col("product_name")))
        .withColumn("device_id", F.coalesce(F.col("device_id_raw"), F.col("install_id")))
        .withColumn(
            "install_source",
            F.when(F.col("install_source") == "com.android.vending", "Google Play")
            .when(F.col("install_source") == "com.google.android.packageinstaller", "Google Play")
            .when(F.col("install_source") == "iTunes", "Apple App Store")
            .when(F.col("install_source") == "com.amazon.venezia", "Amazon Appstore")
            .otherwise("not legit")
        )
        .withColumn(
            "event_key",
            F.md5(
                F.concat_ws(
                    "||",
                    F.col("event_timestamp").cast("string"),
                    F.col("device_id").cast("string"),
                    F.col("event_name").cast("string")
                )
            )
        )
        .dropDuplicates(["event_key"])
    )

    return final_df
