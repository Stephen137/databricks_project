# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data LAke using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret / password for the Application
# MAGIC 3. Set Spark Config with App / Client Id, Directory / Tenant Id & secret
# MAGIC 4. Assign Role "Storage Blob Data Contributor" to the Data Lake
# MAGIC
# MAGIC
# MAGIC This is the RECOMMENDED approval config for the project as it allows :
# MAGIC
# MAGIC - storage of credentials in the Key Vault
# MAGIC - mounting of storage to the Workspace

# COMMAND ----------

client_id = dbutils.secrets.get(scope="f1_project_scope",key="f1-app-client-id" )
tenant_id = dbutils.secrets.get(scope="f1_project_scope",key="f1-app-tenant-id" )
client_secret = dbutils.secrets.get(scope="f1_project_scope",key="f1-app-client-secret")

# COMMAND ----------

# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage#--access-azure-data-lake-storage-gen2-or-blob-storage-using-oauth-20-with-an-azure-service-principal

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1projectdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1projectdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

spark.conf.set("fs.azure.account.oauth2.client.id.f1projectdl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1projectdl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1projectdl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1projectdl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1projectdl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

