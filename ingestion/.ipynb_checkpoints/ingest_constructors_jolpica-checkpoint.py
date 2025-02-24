# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest constructors.JSON

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

constructors_jolpica_df = spark.read.json(f"{raw_folder_path}/constructors_cleaned.json", multiLine=True)

# COMMAND ----------

display(constructors_jolpica_df)

# COMMAND ----------

display(constructors_jolpica_df.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drops columns that we don't need

# COMMAND ----------

constructors_jolpica_dropped_df = constructors_jolpica_df.drop("url")

# COMMAND ----------

display(constructors_jolpica_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC If we invoke the `col` function we can change colum names inline.

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructors_jolpica_dropped_df = constructors_jolpica_df.drop(col("url"))                                 
                                                 


# COMMAND ----------

display(constructors_jolpica_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Adding new columns (ingestion_date)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col

# COMMAND ----------

constructors_jolpica_final_df = constructors_jolpica_dropped_df \
            .withColumnRenamed("constructorId", "constructor_id") \
            .withColumnRenamed("constructorRef","constructor_ref") \
            .withColumn("data_source", lit(v_data_source)) 
            



# COMMAND ----------

constructors_jolpica_final_df = add_ingestion_date(constructors_jolpica_final_df)

# COMMAND ----------

display(constructors_jolpica_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 5 - Write the file to Data Lake as a Delta

# COMMAND ----------

constructors_jolpica_final_df.write \
.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_processed.jolpica_constructors")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Check we can read the Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_processed.jolpica_constructors