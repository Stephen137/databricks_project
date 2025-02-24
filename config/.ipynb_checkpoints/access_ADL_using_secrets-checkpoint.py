# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data LAke using secrets
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

# DBTITLE 1,s
f1projectdl_account_key = dbutils.secrets.get(scope="f1_project_scope", key="f1projectdl-account-key")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.f1projectdl.dfs.core.windows.net", f1projectdl_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1projectdl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1projectdl.dfs.core.windows.net/circuits.csv"))