# Databricks notebook source
# MAGIC %md
# MAGIC ## Constructor Standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results_df = spark.read \
.format("delta") \
.load(f"{presentation_folder_path}/jolpica_race_results")


# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_season')

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_results_df = spark.read \
.format("delta") \
.load(f"{presentation_folder_path}/jolpica_race_results") \
.filter(col("race_season").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

constructor_standings_df = race_results_df \
    .groupBy("race_season", "constructor_name") \
    .agg(sum("points").alias("total_points"), \
        count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

constructor_rank = Window.partitionBy("race_season") \
.orderBy(desc("total_points"), desc("wins")) # tie-breaker = wins



# COMMAND ----------

constructor_standings_final_df = constructor_standings_df \
    .withColumn("rank", rank().over(constructor_rank))

# COMMAND ----------

display(constructor_standings_final_df)

# COMMAND ----------

constructor_standings_final_df.write \
.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_presentation.jolpica_constructor_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_presentation.jolpica_constructor_standings
# MAGIC WHERE race_season = 2022