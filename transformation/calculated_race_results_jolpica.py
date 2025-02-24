%sql
USE f1_presentation;


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