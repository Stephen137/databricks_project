f1projectdl_account_key = dbutils.secrets.get(scope="f1_project_scope", key="f1projectdl-account-key")


spark.conf.set(
    "fs.azure.account.key.f1projectdl.dfs.core.windows.net", f1projectdl_account_key)


display(dbutils.fs.ls("abfss://demo@f1projectdl.dfs.core.windows.net"))


display(spark.read.csv("abfss://demo@f1projectdl.dfs.core.windows.net/circuits.csv"))