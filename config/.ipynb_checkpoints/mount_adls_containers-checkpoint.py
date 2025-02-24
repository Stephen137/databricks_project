# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azuure Data Lake Containers for the F1 project
# MAGIC 1. Get client id, tenant id, and client secret from vault
# MAGIC 2. Set Spark Config with App / Client Id, Directory / Tenant Id & secret
# MAGIC 3. Call file system utility mount to mount the storage - using dictionary structure
# MAGIC 4. Explore other file system utilities related to mount()
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    client_id = dbutils.secrets.get(scope="f1_project_scope",key="f1-app-client-id" )
    tenant_id = dbutils.secrets.get(scope="f1_project_scope",key="f1-app-tenant-id" )
    client_secret = dbutils.secrets.get(scope="f1_project_scope",key="f1-app-client-secret")

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # Mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}", # good practice to reference storage account/container
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("f1projectdl", "raw")

# COMMAND ----------

mount_adls("f1projectdl", "processed")

# COMMAND ----------

mount_adls("f1projectdl", "presentation")

# COMMAND ----------

mount_adls("f1projectdl", "demo")

# COMMAND ----------

dbutils.fs.ls("/mnt/f1projectdl/demo")

# COMMAND ----------

# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage#--access-azure-data-lake-storage-gen2-or-blob-storage-using-oauth-20-with-an-azure-service-principal

# COMMAND ----------

# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts

# COMMAND ----------

# Now that we have mounted the DBFS we can now use the abrreviated file path reference 
display(spark.read.csv("/mnt/f1projectdl/demo/circuits.csv"))