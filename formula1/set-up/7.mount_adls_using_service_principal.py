# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 3. Call file system utlity mount to mount the storage
# MAGIC 4. Explore other file system utlities related to mount (list all mounts, unmount)

# COMMAND ----------

#get client_id,tenant_id and client_Secret from azure portal
client_id = ""
tenant_id = ""
client_secret = "" #ned secret-value

# COMMAND ----------

#adding all the configs in one 
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

#mounting the source to mount_point
dbutils.fs.mount(
  source = "abfss://demo@vsarthidl1.dfs.core.windows.net/",
  mount_point = "/mnt/vsarthidl1/demo",
  extra_configs = configs)

# COMMAND ----------

#displaying the mount
display(dbutils.fs.ls("/mnt/vsarthidl1/demo"))

# COMMAND ----------

#using the mount create by user
display(spark.read.csv("/mnt/vsarthidl1/demo/circuits.csv"))

# COMMAND ----------

#to view all the mounts created
display(dbutils.fs.mounts())

# COMMAND ----------

# unmounting the mounts
dbutils.fs.unmount('/mnt/formula1dl/demo')

# COMMAND ----------


