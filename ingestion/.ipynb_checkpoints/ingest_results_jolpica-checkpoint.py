# Databricks notebook source
# MAGIC %md
# MAGIC ## Jolpica

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest results.JSON

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

jolpica_results_df = spark.read.json(f"{raw_folder_path}/results_cleaned.json")

# COMMAND ----------

display(jolpica_results_df)

# COMMAND ----------

# Explode the `Results` array to create one row per item
from pyspark.sql.functions import col, explode

df_exploded = jolpica_results_df.withColumn("Result", explode(col("Results"))).drop("Results")

# COMMAND ----------

# Flatten the JSON
flattened_df = df_exploded.select(
    col("Circuit.Location.country").alias("country"),
    col("Circuit.Location.lat").alias("latitude"),
    col("Circuit.Location.locality").alias("locality"),
    col("Circuit.Location.long").alias("longitude"),
    col("Circuit.circuitId").alias("circuit_id"),
    col("Circuit.CircuitName").alias("circuit_name"),
    col("Circuit.url").alias("circuit_url"),    
    col("Result.Constructor.constructorId").alias("constructor_id"),
    col("Result.Constructor.name").alias("constructor_name"),
    col("Result.Constructor.nationality").alias("constructor_nationality"),
    col("Result.Constructor.url").alias("constructor_url"),
    col("Result.Driver.code").alias("driver_code"),
    col("Result.Driver.dateOfBirth").alias("driver_dob"),
    col("Result.Driver.driverId").alias("driver_id"),
    col("Result.Driver.familyName").alias("driver_surname"),
    col("Result.Driver.givenName").alias("driver_forename"),  
    col("Result.Driver.nationality").alias("driver_nationality"),
    col("Result.Driver.permanentNumber").alias("driver_permanent_number"),
    col("Result.Driver.url").alias("driver_url"),
    col("Result.FastestLap.AverageSpeed.speed").alias("fastest_lap_speed"),
    col("Result.FastestLap.AverageSpeed.units").alias("fastest_lap_units"),
    col("Result.FastestLap.Time.time").alias("fastest_lap_time"),
    col("Result.FastestLap.lap").alias("fastest_lap"),
    col("Result.FastestLap.rank").alias("fastest_lap_rank"),
    col("Result.Time.millis").alias("fastest_time_ms"),
    col("Result.Time.time").alias("fastest_time_formatted"),    
    col("Result.grid").alias("grid_position"),
    col("Result.laps"),
    col("Result.number"),
    col("Result.points"),
    col("Result.position"),
    col("Result.positionText").alias("position_text"),
    col("Result.status"),
    col("date"),
    col("raceName").alias("race_name"), 
    col("round"),
    col("season"),
    col("time"),
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
jolpica_results_final_df = add_ingestion_date(flattened_dropped_df)

# COMMAND ----------

display(jolpica_results_final_df)

# COMMAND ----------

# extracting number of rows from the Dataframe
number_rows = jolpica_results_final_df.count()

# COMMAND ----------

display(number_rows)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 5 - Write the file to Data Lake as a Delta

# COMMAND ----------

jolpica_results_final_df.write \
.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_processed.jolpica_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Check we can read the Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_processed.jolpica_results