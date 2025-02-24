# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits.csv

# COMMAND ----------

# create a widget which is displayed in top left of Notebook to accept user input
dbutils.widgets.text("p_data_source", "jolpica_api" )
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON using the Spark DataFrame reader

# COMMAND ----------

# MAGIC %md
# MAGIC https://spark.apache.org/docs/latest/api/python/index.html

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2 - Schema

# COMMAND ----------

circuits_jolpica_df = spark.read.json(f"{raw_folder_path}/circuits_cleaned.json", multiLine=True)       

# COMMAND ----------

display(circuits_jolpica_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

flattened_df = circuits_jolpica_df.select(
    col("Location.country").alias("country"),
    col("Location.lat").alias("latitude"),
    col("Location.locality").alias("locality"),
    col("Location.long").alias("longitude"),
    col("circuitId").alias("circuit_id"),
    col("circuitName").alias("circuit_name"),
    col("url").alias("circuit_url")
   )

# COMMAND ----------

display(flattened_df)

# COMMAND ----------

# MAGIC %md
# MAGIC  It is best practice to explicity specify the desired Schema rather than rely on inference. Note that we passed in inferSchema = True when we read the csv, which is a dangerous shortcut.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Select only the columns that we need

# COMMAND ----------

# MAGIC %md
# MAGIC This works fine, but does not allow any further modifications inline, e.g. renaming columns etc.

# COMMAND ----------

circuits_jolpica_selected_df = flattened_df.drop("circuit_url")

# COMMAND ----------

display(circuits_jolpica_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC If we invoke the `col` function we can change colum names inline.

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

# MAGIC %md 
# MAGIC There is another way to change column names, and that is to invoke the`.withColumnRenamed()` function.

# COMMAND ----------

circuits_jolpica_renamed_df = circuits_jolpica_selected_df \
.withColumn("data_source", lit(v_data_source))
       

# COMMAND ----------

display(circuits_jolpica_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Adding a new column (Ingestion Date)

# COMMAND ----------

# call custom function to add current date
circuits_jolpica_final_df = add_ingestion_date(circuits_jolpica_renamed_df)

# COMMAND ----------

display(circuits_jolpica_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 5 - Write the file to Data Lake as a Delta file

# COMMAND ----------

circuits_jolpica_final_df.write \
.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_processed.circuits_jolpica")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Check we can read the delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_processed.circuits_jolpica

# COMMAND ----------

# MAGIC %md
# MAGIC