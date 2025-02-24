sas_token = dbutils.secrets.get(scope="f1_project_scope", key="f1-demo-SAS-token")


spark.conf.set("fs.azure.account.auth.type.f1projectdl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1projectdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1projectdl.dfs.core.windows.net", sas_token)


display(dbutils.fs.ls("abfss://demo@f1projectdl.dfs.core.windows.net"))

display(spark.read.csv("abfss://demo@f1projectdl.dfs.core.windows.net/circuits.csv"))



