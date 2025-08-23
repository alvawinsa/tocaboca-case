# Databricks notebook source
# MAGIC %run /Users/winsa.alva@gmail.com/tocaboca-case/ingestion/includes/configuration

# COMMAND ----------

# This script only needs to be run once, to mount the files from Azure storage container to abfs
def mount_adls(storage_account_name, container_name):
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": CLIENT_ID,
          "fs.azure.account.oauth2.client.secret": CLIENT_SECRET,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"}
    
    # Just make sure to unmount if it alreaady exists, so it doesn't fail
    if any (mount.mountPoint == f"/mnt/{STORAGE_ACCOUNT_NAME}/{CONTAINER_NAME}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{STORAGE_ACCOUNT_NAME}/{CONTAINER_NAME}")
    
    # Go through and mount all the containers
    dbutils.fs.mount(
        source = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/",
        mount_point = f"/mnt/{STORAGE_ACCOUNT_NAME}/{CONTAINER_NAME}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls(STORAGE_ACCOUNT_NAME, CONTAINER_NAME)
