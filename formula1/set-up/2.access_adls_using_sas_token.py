# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.vsarthidl1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.vsarthidl1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.vsarthidl1.dfs.core.windows.net", "sp=rl&st=2025-07-23T02:36:43Z&se=2025-07-23T10:51:43Z&spr=https&sv=2024-11-04&sr=c&sig=ST%2FzWwkKAk2KrwxRjSDvVTeniXGDsNENz700mxBZlsE%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@vsarthidl1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@vsarthidl1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


