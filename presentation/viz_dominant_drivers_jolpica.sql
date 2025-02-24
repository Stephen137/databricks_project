-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS

SELECT
  driver_name,
  COUNT(1) AS number_races,
  SUM(calculated_points) AS total_points_rebased,
  AVG(calculated_points) AS average_points_rebased,
  RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS driver_rank

FROM 
  f1_presentation.calculated_race_results_jolpica

GROUP BY
  driver_name 

HAVING 
  number_races >= 50

ORDER BY
  average_points_rebased DESC

-- COMMAND ----------

SELECT
  race_year,
  driver_name,
  COUNT(1) AS number_races,
  SUM(calculated_points) AS total_points_rebased,
  AVG(calculated_points) AS average_points_rebased

FROM 
  f1_presentation.calculated_race_results_jolpica

WHERE 
  driver_name IN 
  (SELECT driver_name 
  FROM v_dominant_drivers
  WHERE driver_rank <= 10)

GROUP BY
  race_year, driver_name 

ORDER BY
  race_year, average_points_rebased DESC

-- COMMAND ----------

SELECT
  driver_name,
  COUNT(1) AS number_races,
  SUM(calculated_points) AS total_points_rebased,
  total_points_rebased / COUNT(1) AS average_points_rebased
FROM 
  f1_presentation.calculated_race_results_jolpica
GROUP BY
  driver_name 
HAVING 
  number_races >= 50
ORDER BY
  average_points_rebased DESC
LIMIT 10

-- COMMAND ----------

SELECT
  driver_name, 
  COUNT(*) AS total_podium_finishes  

FROM 
  f1_presentation.calculated_race_results_jolpica
WHERE 

  position <= 3

GROUP BY
  driver_name

ORDER BY
  total_podium_finishes DESC

LIMIT 10


