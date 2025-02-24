from pyspark.sql.functions import col, lit

%run "../includes/configuration"

%run "../includes/common_functions"

# create a widget which is displayed in top left of Notebook to accept user input
dbutils.widgets.text("p_data_source", "jolpica_api" )
v_data_source = dbutils.widgets.get("p_data_source")

circuits_jolpica_df = spark.read.json(f"{raw_folder_path}/circuits_cleaned.json", multiLine=True)       

flattened_df = circuits_jolpica_df.select(
    col("Location.country").alias("country"),
    col("Location.lat").alias("latitude"),
    col("Location.locality").alias("locality"),
    col("Location.long").alias("longitude"),
    col("circuitId").alias("circuit_id"),
    col("circuitName").alias("circuit_name"),
    col("url").alias("circuit_url")
   )

circuits_jolpica_selected_df = flattened_df.drop("circuit_url")

circuits_jolpica_renamed_df = circuits_jolpica_selected_df \
.withColumn("data_source", lit(v_data_source))      

# call custom function to add current date
circuits_jolpica_final_df = add_ingestion_date(circuits_jolpica_renamed_df)

# write file to Data Lake as a Delta
circuits_jolpica_final_df.write \
.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_processed.circuits_jolpica")
