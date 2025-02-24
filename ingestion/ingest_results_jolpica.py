from pyspark.sql.functions import col, explode, lit

%run "../includes/configuration"

%run "../includes/common_functions"

# create a widget which is displayed in top left of Notebook to accept user input
dbutils.widgets.text("p_data_source", "" )
v_data_source = dbutils.widgets.get("p_data_source")

jolpica_results_df = spark.read.json(f"{raw_folder_path}/results_cleaned.json")

df_exploded = jolpica_results_df.withColumn("Result", explode(col("Results"))).drop("Results")

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

# include a column to show data source
flattened_renamed_df = flattened_df.withColumn("data_source", lit(v_data_source)) 

# drop columns not required
flattened_dropped_df = flattened_renamed_df.drop("driver_url", "constructor_url", "circuit_url", "race_url")

# add a column to show ingestion date
jolpica_results_final_df = add_ingestion_date(flattened_dropped_df)

# Write the file to Data Lake as a Delta
jolpica_results_final_df.write \
.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_processed.jolpica_results")