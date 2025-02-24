# Databricks notebook source
# create a widget which is displayed in top left of Notebook to accept user input
dbutils.widgets.text("p_data_source", "jolpica_api" )
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results_df = spark.read \
.format("delta") \
.load(f"{presentation_folder_path}/jolpica_race_results") 

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, "race_season")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, concat, lit

# COMMAND ----------

race_results_df = spark.read \
    .format("delta") \
    .load(f"{presentation_folder_path}/jolpica_race_results") \
    .withColumn("driver_name", concat(col("driver_first_name"), lit(" "), col("driver_surname"))) \
    .filter(col("race_season").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

driver_standings_df = race_results_df \
    .groupBy("race_season", "driver_name", "driver_nationality") \
    .agg(sum("points").alias("total_points"), \
        count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank = Window.partitionBy("race_season") \
.orderBy(desc("total_points"), desc("wins")) # tie-breaker = wins



# COMMAND ----------

driver_standings_final_df = driver_standings_df \
    .withColumn("rank", rank().over(driver_rank))

# COMMAND ----------

display(driver_standings_final_df)

# COMMAND ----------

driver_standings_final_df.write \
.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_presentation.jolpica_driver_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_presentation.jolpica_driver_standings
# MAGIC WHERE race_season = 2022