# Databricks notebook source
# This script only needs to be run once, to mount the files from Azure storage container to abfs
def mount_adls(storage_account_name, container_name):
    client_secret = dbutils.secrets.get(
        scope="awanalytics-scope",
        key="analytics-sp-secret"
    )
    client_id = dbutils.secrets.get(
        scope="awanalytics-scope",
        key="awanalytics-client-id"
        )
    tenant_id = dbutils.secrets.get(
        scope="awanalytics-scope",
        key="awanalytics-tenant-id"
    )

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Just make sure to unmount if it alreaady exists, so it doesn't fail
    if any (mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Go through and mount all the containers
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("analyticsaw","tocaboca")
