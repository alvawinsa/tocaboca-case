# Databricks notebook source
# Path to mounted container
FOLDER_PATH = "/mnt/analyticsaw/tocaboca"

# Path to unity catalog and table location
BRONZE_SCHEMA = "prod_bronze.tocaboca"
EVENTS_TABLE = "events"
PRODUCTS_TABLE = "products"
EXCHANGE_TABLE = "exchange_rates"

# Azure storage details
STORAGE_ACCOUNT_NAME = "analyticsaw"
CONTAINER_NAME = "tocaboca"

# Secrets from Azure key vault
CLIENT_SECRET = dbutils.secrets.get(
    scope="awanalytics-scope", key="analytics-sp-secret"
)
CLIENT_ID = dbutils.secrets.get(scope="awanalytics-scope", key="awanalytics-client-id")
TENANT_ID = dbutils.secrets.get(scope="awanalytics-scope", key="awanalytics-tenant-id")
