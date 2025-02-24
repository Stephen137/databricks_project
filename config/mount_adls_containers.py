# https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage#--access-azure-data-lake-storage-gen2-or-blob-storage-using-oauth-20-with-an-azure-service-principal

# https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts

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
    






