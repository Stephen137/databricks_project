from pyspark.sql.functions import col, lit

%run "../includes/configuration"

%run "../includes/common_functions"

# create a widget which is displayed in top left of Notebook to accept user input
dbutils.widgets.text("p_data_source", "" )
v_data_source = dbutils.widgets.get("p_data_source")

jolpica_races_df = spark.read.json(f"{raw_folder_path}/races_cleaned.json", multiLine=True)

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

flattened_renamed_df = flattened_df.withColumn("data_source", lit(v_data_source)) 

# drop columns not required
flattened_dropped_df = flattened_renamed_df.drop("driver_url", "constructor_url", "circuit_url", "race_url")

# add a column to show ingestion date
jolpica_races_final_df = add_ingestion_date(flattened_dropped_df)

# Write the file to Data Lake as a Delta Table
jolpica_races_final_df.write \
.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_processed.jolpica_races")
