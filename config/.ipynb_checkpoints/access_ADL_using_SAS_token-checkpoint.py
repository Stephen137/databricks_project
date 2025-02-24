# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data LAke using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

sas_token = dbutils.secrets.get(scope="f1_project_scope", key="f1-demo-SAS-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1projectdl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1projectdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1projectdl.dfs.core.windows.net", sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1projectdl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1projectdl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

