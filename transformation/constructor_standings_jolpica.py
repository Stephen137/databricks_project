# Databricks notebook source
from pyspark.sql.functions import sum, when, count, col, desc, rank
from pyspark.sql.window import Window

%run "../includes/configuration"

%run "../includes/common_functions"

race_results_df = spark.read \
.format("delta") \
.load(f"{presentation_folder_path}/jolpica_race_results")

race_year_list = df_column_to_list(race_results_df, 'race_season')

race_results_df = spark.read \
.format("delta") \
.load(f"{presentation_folder_path}/jolpica_race_results") \
.filter(col("race_season").isin(race_year_list))

constructor_standings_df = race_results_df \
    .groupBy("race_season", "constructor_name") \
    .agg(sum("points").alias("total_points"), \
        count(when(col("position") == 1, True)).alias("wins"))

constructor_rank = Window.partitionBy("race_season") \
.orderBy(desc("total_points"), desc("wins")) # tie-breaker = wins


constructor_standings_final_df = constructor_standings_df \
    .withColumn("rank", rank().over(constructor_rank))

# write file to Data Lake as a Delta
constructor_standings_final_df.write \
.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_presentation.jolpica_constructor_standings")