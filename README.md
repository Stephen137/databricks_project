# F1 DataBricks Project

![F1_Pipeline.JPG](img/0f5f98f3-9338-48c4-be80-64e63e7b662a.JPG)

In this project I will:

- obtain data from the Jolpica F1 API
- store the data in Azure Data Lake Storage in Delta format
- follow the medallion architecture, i.e. create three separate layers ***raw***, ***processed***, and ***presentation***
- clean and transform the data using ***Spark*** and ***SQL*** within DataBricks Notebooks
- orchestrate a fully automated end-to-end pipeline with a trigger and scheduling of Notebook runs, using Azure Data Factory
- visualise the data using the inbuilt Databricks visualization tool

![driver_dominance_area_chart.JPG](img/e71dbac3-a9ce-4178-9479-6c334a4f03cd.JPG)

![constructor_dominance_area_chart.JPG](img/5f1c4bfa-905c-42f6-96c7-63c1d55f6e71.JPG)


## Jolpica API

https://github.com/jolpica/jolpica-f1/tree/main/docs

## Microsoft Azure 

Microsoft Azure, formerly known as Windows Azure, is Microsoft's public cloud computing platform. It provides a broad range of cloud services, including compute, analytics, storage and networking. Users can choose from these services to develop and scale new applications or run existing applications in the public cloud.

Microsoft charges for Azure on a pay-as-you-go (PAYG) basis, meaning subscribers receive a bill each month that only charges them for the specific resources and services they have used, howvever I took advantage of the [14 day free trial](https://azure.microsoft.com/en-gb/pricing/offers/ms-azr-0044p). which includes the equivalent of $200 credit.

### Configuration

As with all cloud platforms, there's a fair bit of setting up to do in terms of which resources you want to choose, access control via a Key Vault, verification procedures etc.

### Create a storage account

![create_storage_account.JPG](img/4b121969-1b4d-4e7b-a5a8-1d52e978910e.JPG)

![data_lake_console.JPG](img/b157f45c-38e9-4f6c-9eb7-62985113d302.JPG)

![storage_explorer.JPG](img/30e648e9-5835-4634-8ceb-091d215e3299.JPG)

## Databricks

Databricks, Inc. is a global data, analytics, and artificial intelligence (AI) company, founded in 2013 by the original creators of Apache Spark.The company provides a cloud-based platform to help enterprises build, scale, and govern data and AI, including generative AI and other machine learning models.

Databricks pioneered the data lakehouse, a data and AI platform that combines the capabilities of a data warehouse with a data lake, allowing organizations to manage and use both structured and unstructured data for traditional business analytics and AI workloads. The company similarly develops Delta Lake, an open-source project to bring reliability to data lakes for machine learning and other data science use cases.

![Azure_DataBricks.JPG](img/927e1ab5-7714-46a8-b30a-67b605bbe2f2.JPG)

![create_DataBricks_workspace.JPG](img/afebd93f-6eec-45ff-bf89-c39e610b2c21.JPG)

![Azure_DataBricks_Workspace.JPG](img/efa2542f-af23-407b-80d5-3559d9c91263.JPG)

![DataBricks_console.JPG](img/fe50181a-8e71-4712-8c12-2aa52917bd77.JPG)

### Clusters

![create_cluster.JPG](img/d643774a-2e79-4020-bfe6-1b1bc78560ff.JPG)

![cluster_policy.JPG](img/92ace34a-a637-4446-8e13-5dd919aec05e.JPG)

![cluster_policy_JSON.JPG](img/d5be2139-6cac-48a5-b752-5e86546c0090.JPG)

![enable_DBFS.JPG](img/8ed84901-b7ff-46b2-85ab-cf8b8b69d783.JPG)

## Databricks NoteBooks

One of the fantastic features is the ability to switch languages within the Notebook.

![databricks_languages.JPG](img/bd203142-1a10-470b-ab0d-c20cb1b045a0.JPG)

## Connect to Microsoft Azure Storage Containers

https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage#--access-azure-data-lake-storage-gen2-or-blob-storage-using-oauth-20-with-an-azure-service-principal



```python
client_id = dbutils.secrets.get(scope="f1_project_scope",key="f1-app-client-id" )
tenant_id = dbutils.secrets.get(scope="f1_project_scope",key="f1-app-tenant-id" )
client_secret = dbutils.secrets.get(scope="f1_project_scope",key="f1-app-client-secret")

spark.conf.set("fs.azure.account.auth.type.f1projectdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1projectdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

spark.conf.set("fs.azure.account.oauth2.client.id.f1projectdl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1projectdl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1projectdl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

display(dbutils.fs.ls("abfss://demo@f1projectdl.dfs.core.windows.net"))

display(spark.read.csv("abfss://demo@f1projectdl.dfs.core.windows.net/circuits.csv"))
```

### Mount Storage Containers and assign paths to variables

https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts


```python
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
```

## Apache Spark

https://spark.apache.org/docs/3.5.4/api/python/reference/pyspark.sql/index.html

![Apache_Spark_architecture.JPG](img/f4b8eb2a-177a-4ae4-9fde-9b025f689fb4.JPG)

## Azure Data Factory

[Azure Data Factory (ADF)](img/https://azure.microsoft.com/en-us/products/data-factory) is a fully managed, serverless data integration solution. I leveraged ADF to create a scalable pipeline to incrementally ingest, transform and present the data that I obtained from the Jolpica API.

This was fully automated making use of the trigger feature and took just two and a half minutes to complete.

![azure_data_factory.JPG](img/b3f59252-b6cb-42c2-ae10-44b406d34484.JPG)

### Master Pipeline

![master_pipeline.JPG](img/1cc3b061-2607-43c4-998d-dc2172ef931e.JPG)

### Ingestion Pipeline

![ingest_pipeline.JPG](img/39381fb8-39e5-493a-bb9a-1c602db1527f.JPG)

### Transformation Pipeline

![tramsform__pipeline.JPG](img/92d85fb1-15c1-4500-8c9e-793cccb947b0.JPG)

### Presentation Pipeline

![presentation_pipeline.JPG](img/65b6e026-9267-4f86-823f-7f81543e8d4b.JPG)

### Creating a Trigger

![trigger.JPG](img/7cfbfb53-6959-447e-970f-5dfdf2487c5f.JPG)

![pipeline_gant.JPG](img/5df7b926-7ea2-4b96-8d2a-c04d7e1ba82e.JPG)

## Data Viz

DataBricks includes a very good in-built visualisation tool which allows you to create charts and dashboards from within the notebook.

![viz_data.JPG](img/c7de3f62-8f3f-4c4c-9322-bf70d8f9c941.JPG)

![viz_editor.JPG](img/6cf92e72-1978-497f-bebe-9bc54dd948fb.JPG)


### Drivers

![top_10_drivers.png](img/60901867-3e13-4fb3-95ab-9a338fbb0274.png)

![driver_podium_finishes.png](img/91fc33f3-46b9-41ad-94b6-99a30e48f754.png)


### Constructors

![top_5_constructors.png](img/a3b1d12c-c23f-456a-830f-28b53e0176c3.png)

![constructor_podium_finishes.png](img/e6030580-5ec7-4566-881c-cb028c2b2d7b.png)

### Connecting to Other BI tools
You can also connect the data from DataBricks to a vast choice of connectors, e.g. Power BI, Tableau etc.

![connect_to_power_BI.JPG](img/9ef645b1-c6da-46ae-a894-ec25f080da8f.JPG)

To do this, you will generally need the Server Hostname and HTTP Path:

![server_HTTP_path.JPG](img/948cf5a2-8e06-4d83-a68e-abc8921ff625.JPG)



