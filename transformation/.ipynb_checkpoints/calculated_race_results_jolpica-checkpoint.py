# Databricks notebook source
# MAGIC %md
# MAGIC ## Rebasing points awarded to enable comparison across seasons

# COMMAND ----------

# MAGIC %md
# MAGIC  In recent years drivers have been awarded 25 points for a win, but in ther 1950s for example a driver would only get say 8 or 10 points for winning a race.
# MAGIC
# MAGIC  In order to make a meaningful comparison across history, we need to rebase the points awarded.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE f1_presentation;

# COMMAND ----------

# create intitial table
spark.sql(f"""
        CREATE OR REPLACE TABLE f1_presentation.calculated_race_results_jolpica 
AS

SELECT
  jolpica_races.race_date AS race_date,
  jolpica_races.race_season AS race_year,
  jolpica_constructors.name AS team_name,
  jolpica_drivers.driver_id AS driver_id,
  concat(jolpica_drivers.first_name, " ", jolpica_drivers.surname) AS driver_name,
  jolpica_results.position,
  jolpica_results.points,
  11 - jolpica_results.position AS calculated_points --this rebases points awarded for a win to 10

FROM 
  f1_processed.jolpica_results

JOIN 
  f1_processed.jolpica_drivers ON (jolpica_results.driver_id = jolpica_drivers.driver_id)

JOIN 
  f1_processed.jolpica_constructors ON (jolpica_results.constructor_id = jolpica_constructors.constructor_id)

JOIN 
  f1_processed.jolpica_races ON (jolpica_results.date = jolpica_races.race_date)

WHERE
  jolpica_results.position <= 10

""")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Using SQL syntax

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.calculated_race_results_jolpica;