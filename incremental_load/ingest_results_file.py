# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %sql
# MAGIC USE f1_processed;

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2025-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.read.json(f"dbfs:/FileStore/raw/{v_file_date}/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceId, COUNT(1)
# MAGIC FROM results_cutover
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC

# COMMAND ----------

spark.read.json("dbfs:/FileStore/raw/2021-03-28/results.json").createOrReplaceTempView("results_w1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceId, COUNT(1)
# MAGIC FROM results_w1
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC

# COMMAND ----------

spark.read.json("dbfs:/FileStore/raw/2025-04-18/results.json").createOrReplaceTempView("results_w2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceId, COUNT(1)
# MAGIC FROM results_w2
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"dbfs:/FileStore/raw/{v_file_date}/results.json")

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

for race_id_list in results_final_df.select("race_id").distinct().collect():
    spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results
# MAGIC -- this shows that only first race is loaded next we need to load the second race and then merge the data
# MAGIC -- after we changed the date of the data, and runs the code again then the table gets the data of the new date added to the table as we have used the command append with the saveAsTable.

# COMMAND ----------

# MAGIC %md
# MAGIC **Problems**
# MAGIC - sometimes if we load the data again and again 

# COMMAND ----------

# MAGIC %md
# MAGIC there is one more methos for increamental load that is the use od "INSERT INTO" with the "overwrite".
