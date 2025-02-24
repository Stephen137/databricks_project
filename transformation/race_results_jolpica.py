# Databricks notebook source
# create a widget which is displayed in top left of Notebook to accept user input
dbutils.widgets.text("p_data_source", "jolpica_api" )
v_data_source = dbutils.widgets.get("p_data_source")

%run "../includes/configuration"

%run "../includes/common_functions"

# read from delta
races_df =spark.read \
    .format("delta") \
    .load(f"{processed_folder_path}/jolpica_races") 

# read from delta
circuits_df =spark.read \
    .format("delta") \
    .load(f"{processed_folder_path}/circuits_jolpica")   


# read from delta
drivers_df =spark.read \
.format("delta") \
.load(f"{processed_folder_path}/jolpica_drivers") \
.withColumnRenamed("surname","driver_surname") \
.withColumnRenamed("first_name","driver_first_name") \
.withColumnRenamed("nationality", "driver_nationality")


# read from delta
constructors_df =spark.read \
.format("delta") \
.load(f"{processed_folder_path}/jolpica_constructors") \
.withColumnRenamed("name","constructor_name")


# read from delta
results_df =spark.read \
.format("delta") \
.load(f"{processed_folder_path}/jolpica_results") \
.withColumnRenamed("number","lap_number") \
.withColumnRenamed("date","race_date") \
.withColumnRenamed("time","race_time") \
.withColumnRenamed("status","race_status")


# Join races to circuits
races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id) \
    .select(races_df.race_date, races_df.race_season, races_df.race_name, races_df.race_round, circuits_df.circuit_name)

# Join other tables
race_results_df = results_df.join \
    (races_circuits_df, results_df.race_date == races_circuits_df.race_date) \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id) \
    .select(results_df.race_date, 'race_season', results_df.race_name, races_circuits_df.circuit_name, "driver_first_name", drivers_df.driver_surname, "permanent_driver_number", drivers_df.driver_nationality, constructors_df.constructor_name, "grid_position", "fastest_lap_time", "race_time","points", "position","race_status")    
                                

race_results_final_df = add_ingestion_date(race_results_df) \
    .withColumnRenamed("ingestion_date", "created_date") 


# write file to Data Lake as a Delta
race_results_final_df.write \
.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_presentation.jolpica_race_results")