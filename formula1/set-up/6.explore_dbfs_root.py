# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore DBFS Root
# MAGIC 1. List all the folders in DBFS root
# MAGIC 2. Interact with DBFS File Browser
# MAGIC 3. Upload file to DBFS Root

# COMMAND ----------

#we have these directories within dbfs root
display(dbutils.fs.ls('/'))

#for it to show dbfs 

# COMMAND ----------

#checking what are the files present in /FileStore
display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('dbfs:/FileStore/circuits__1_.csv'))

# COMMAND ----------


