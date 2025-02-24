# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest NESTED drivers.JSON

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
# MAGIC ### Step 1 - Read the nested JSON file using the Spark DataFrame reader

# COMMAND ----------

# MAGIC %md
# MAGIC https://spark.apache.org/docs/latest/api/python/index.html

# COMMAND ----------

jolpica_drivers_df = spark.read.json(f"{raw_folder_path}/drivers_cleaned.json", multiLine=True)

# COMMAND ----------

display(jolpica_drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

jolpica_drivers_with_columns_df = jolpica_drivers_df.withColumnRenamed("code", "driver_code") \
                                    .withColumnRenamed("dateOfBirth", "date_of_birth") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("familyName", "surname") \
                                    .withColumnRenamed("givenName", "first_name") \
                                    .withColumnRenamed("nationality", "nationality") \
                                    .withColumnRenamed("permanentNumber", "permanent_driver_number") \
                                    .withColumnRenamed("url", "url") \
                                    .withColumn("data_source", lit(v_data_source)) 
                                    


# COMMAND ----------

display(jolpica_drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop unwanted columns

# COMMAND ----------

jolpica_drivers_with_columns_df = jolpica_drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add ingestion date using Custom Function

# COMMAND ----------

jolpica_drivers_final_df = add_ingestion_date(jolpica_drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 5 - Write the file to Data Lake as Delta

# COMMAND ----------

jolpica_drivers_final_df.write \
.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_processed.jolpica_drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Check we can read the Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_processed.jolpica_drivers