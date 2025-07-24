# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()
#all the secrets present in our scope

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')
#it expects parameter to be passed

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1-scope', key = 'vsarthidl1-account-key')

# COMMAND ----------


