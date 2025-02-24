from pyspark.sql.functions import sum, when, count, col, concat, lit, desc, rank
from pyspark.sql.window import Window

# Databricks notebook source
# create a widget which is displayed in top left of Notebook to accept user input
dbutils.widgets.text("p_data_source", "jolpica_api" )
v_data_source = dbutils.widgets.get("p_data_source")

%run "../includes/configuration"

%run "../includes/common_functions"

race_results_df = spark.read \
.format("delta") \
.load(f"{presentation_folder_path}/jolpica_race_results") 

race_year_list = df_column_to_list(race_results_df, "race_season")

race_results_df = spark.read \
    .format("delta") \
    .load(f"{presentation_folder_path}/jolpica_race_results") \
    .withColumn("driver_name", concat(col("driver_first_name"), lit(" "), col("driver_surname"))) \
    .filter(col("race_season").isin(race_year_list))


driver_standings_df = race_results_df \
    .groupBy("race_season", "driver_name", "driver_nationality") \
    .agg(sum("points").alias("total_points"), \
        count(when(col("position") == 1, True)).alias("wins"))

driver_rank = Window.partitionBy("race_season") \
.orderBy(desc("total_points"), desc("wins")) # tie-breaker = wins

driver_standings_final_df = driver_standings_df \
    .withColumn("rank", rank().over(driver_rank))

# write file to Data Lake as a Delta
driver_standings_final_df.write \
.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_presentation.jolpica_driver_standings")