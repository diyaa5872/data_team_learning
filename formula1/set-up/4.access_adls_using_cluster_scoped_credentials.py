# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

#Set the spark config fs.azure.account.key in the cluster
spark.conf.set("fs.azure.account.auth.type.vsarthidl1.dfs.core.windows.net","")

# COMMAND ----------

#List files from demo container
display(dbutils.fs.ls("abfss://demo@vsarthidl1.dfs.core.windows.net"))

# COMMAND ----------

#Read data from circuits.csv file
display(spark.read.csv("abfss://demo@vsarthidl1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


