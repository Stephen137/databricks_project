
client_id = dbutils.secrets.get(scope="f1_project_scope",key="f1-app-client-id" )
tenant_id = dbutils.secrets.get(scope="f1_project_scope",key="f1-app-tenant-id" )
client_secret = dbutils.secrets.get(scope="f1_project_scope",key="f1-app-client-secret")

# https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage#--access-azure-data-lake-storage-gen2-or-blob-storage-using-oauth-20-with-an-azure-service-principal


spark.conf.set("fs.azure.account.auth.type.f1projectdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1projectdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

spark.conf.set("fs.azure.account.oauth2.client.id.f1projectdl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1projectdl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1projectdl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

display(dbutils.fs.ls("abfss://demo@f1projectdl.dfs.core.windows.net"))

display(spark.read.csv("abfss://demo@f1projectdl.dfs.core.windows.net/circuits.csv"))



