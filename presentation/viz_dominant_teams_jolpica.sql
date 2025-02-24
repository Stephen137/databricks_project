-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS

SELECT
  team_name,
  COUNT(1) AS number_races,
  SUM(calculated_points) AS total_points_rebased,
  AVG(calculated_points) AS average_points_rebased,
  RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS team_rank

FROM 
  f1_presentation.calculated_race_results_jolpica

GROUP BY
  team_name 

HAVING 
  number_races >= 100

ORDER BY
  average_points_rebased DESC

-- COMMAND ----------

SELECT
  race_year,
  team_name,
  COUNT(1) AS number_races,
  SUM(calculated_points) AS total_points_rebased,
  AVG(calculated_points) AS average_points_rebased

FROM 
  f1_presentation.calculated_race_results_jolpica

WHERE 
  team_name IN 
  (SELECT team_name 
  FROM v_dominant_teams
  WHERE team_rank <= 5)

GROUP BY
  race_year, team_name 

ORDER BY
  race_year, average_points_rebased DESC

-- COMMAND ----------

SELECT
  team_name,
  COUNT(1) AS number_races,
  SUM(calculated_points) AS total_points_rebased,
  total_points_rebased / COUNT(1) AS average_points_rebased
FROM 
  f1_presentation.calculated_race_results_jolpica
GROUP BY
  team_name 
HAVING 
  number_races >= 50
ORDER BY
  average_points_rebased DESC
LIMIT 5

-- COMMAND ----------

SELECT
  team_name, 
  COUNT(*) AS total_podium_finishes  

FROM 
  f1_presentation.calculated_race_results_jolpica

WHERE 
  position <= 3

GROUP BY
  team_name

ORDER BY
  total_podium_finishes DESC

LIMIT 5