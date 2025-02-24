from pyspark.sql.functions import col, concat, current_timestamp, lit

%run "../includes/configuration"
    
%run "../includes/common_functions"

# create a widget which is displayed in top left of Notebook to accept user input
dbutils.widgets.text("p_data_source", "" )
v_data_source = dbutils.widgets.get("p_data_source")

jolpica_drivers_df = spark.read.json(f"{raw_folder_path}/drivers_cleaned.json", multiLine=True)

jolpica_drivers_with_columns_df = jolpica_drivers_df.withColumnRenamed("code", "driver_code") \
                                    .withColumnRenamed("dateOfBirth", "date_of_birth") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("familyName", "surname") \
                                    .withColumnRenamed("givenName", "first_name") \
                                    .withColumnRenamed("nationality", "nationality") \
                                    .withColumnRenamed("permanentNumber", "permanent_driver_number") \
                                    .withColumnRenamed("url", "url") \
                                    .withColumn("data_source", lit(v_data_source)) 

jolpica_drivers_with_columns_df = jolpica_drivers_with_columns_df.drop(col("url"))

jolpica_drivers_final_df = add_ingestion_date(jolpica_drivers_with_columns_df)

# Write the file to Data Lake as Delta
jolpica_drivers_final_df.write \
.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_processed.jolpica_drivers")
