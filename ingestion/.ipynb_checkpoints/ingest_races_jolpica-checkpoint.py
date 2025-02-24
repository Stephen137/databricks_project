# Databricks notebook source
# MAGIC %md
# MAGIC ## Jolpica

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest races.JSON

# COMMAND ----------

# create a widget which is displayed in top left of Notebook to accept user input
dbutils.widgets.text("p_data_source", "" )
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the single line JSON using the Spark DataFrame reader

# COMMAND ----------

# MAGIC %md
# MAGIC https://spark.apache.org/docs/latest/api/python/index.html

# COMMAND ----------

jolpica_races_df = spark.read.json(f"{raw_folder_path}/races_cleaned.json", multiLine=True)

# COMMAND ----------

display(jolpica_races_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Flatten the JSON
flattened_df = jolpica_races_df.select(
    col("Circuit.Location.country").alias("country"),
    col("Circuit.Location.lat").alias("latitude"),
    col("Circuit.Location.locality").alias("locality"),
    col("Circuit.Location.long").alias("longitude"),
    col("Circuit.circuitId").alias("circuit_id"),
    col("Circuit.CircuitName").alias("circuit_name"),
    col("Circuit.url").alias("circuit_url"),   
    col("FirstPractice.date").alias("first_practice_date"),
    col("FirstPractice.time").alias("first_practice_time"),
    col("Qualifying.date").alias("qualifying_date"),
    col("Qualifying.time").alias("qualifying_time"),
    col("SecondPractice.date").alias("second_practice_date"),
    col("SecondPractice.time").alias("second_practice_time"),
    col("Sprint.date").alias("sprint_date"),
    col("Sprint.time").alias("sprint_time"),
    col("SprintQualifying.date").alias("sprint_qualifying_date"),
    col("SprintQualifying.time").alias("sprint_qualifying_time"),
    col("SprintShootout.date").alias("sprint_shootout_date"),
    col("SprintShootout.time").alias("sprint_shootout_time"),
    col("ThirdPractice.date").alias("third_practice_date"),
    col("ThirdPractice.time").alias("third_practice_time"),
    col("date").alias("race_date"),
    col("raceName").alias("race_name"), 
    col("round").alias("race_round"),
    col("season").alias("race_season"), 
    col("time").alias("race_time"),
    col("url").alias("race_url")  
)

# COMMAND ----------

# Show the results
display(flattened_df)

# COMMAND ----------

display(flattened_df.printSchema())

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# include a column to show data source
flattened_renamed_df = flattened_df.withColumn("data_source", lit(v_data_source)) 

# COMMAND ----------

# drop columns not required
flattened_dropped_df = flattened_renamed_df.drop("driver_url", "constructor_url", "circuit_url", "race_url")

# COMMAND ----------

# add a column to show ingestion date
jolpica_races_final_df = add_ingestion_date(flattened_dropped_df)

# COMMAND ----------

display(jolpica_races_final_df)

# COMMAND ----------

# extracting number of rows from the Dataframe
number_rows = jolpica_races_final_df.count()

# COMMAND ----------

display(number_rows)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 5 - Write the file to Data Lake as a Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_processed.jolpica_races;

# COMMAND ----------

jolpica_races_final_df.write \
.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_processed.jolpica_races")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Check we can read the Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_processed.jolpica_races