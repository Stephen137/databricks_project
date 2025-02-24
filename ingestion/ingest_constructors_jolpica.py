from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col

# create a widget which is displayed in top left of Notebook to accept user input
dbutils.widgets.text("p_data_source", "" )
v_data_source = dbutils.widgets.get("p_data_source")

%run "../includes/configuration"
    
%run "../includes/common_functions"

constructors_jolpica_df = spark.read.json(f"{raw_folder_path}/constructors_cleaned.json", multiLine=True)

constructors_jolpica_dropped_df = constructors_jolpica_df.drop("url")
                           

constructors_jolpica_final_df = constructors_jolpica_dropped_df \
            .withColumnRenamed("constructorId", "constructor_id") \
            .withColumnRenamed("constructorRef","constructor_ref") \
            .withColumn("data_source", lit(v_data_source)) 
            

constructors_jolpica_final_df = add_ingestion_date(constructors_jolpica_final_df)

# Write the file to Data Lake as a Delta
constructors_jolpica_final_df.write \
.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_processed.jolpica_constructors")