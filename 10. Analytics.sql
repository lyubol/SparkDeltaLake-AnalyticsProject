-- Databricks notebook source
SELECT
  Date,
  Value - LAG(Value, 1) OVER(ORDER BY Value ASC) AS DailyValue
FROM 
  covid.cases 
WHERE 
  Country = 'Bulgaria' 
ORDER BY 
  Date ASC

-- COMMAND ----------

SELECT
  c.Date,
  c.Value - LAG(c.Value, 1) OVER(ORDER BY c.Value ASC) AS CasesDailyValue,
  d.Value - LAG(d.Value, 1) OVER(ORDER BY d.Value ASC) AS DeathsDailyValue
FROM 
  covid.cases AS C
  INNER JOIN covid.deaths AS d
  ON c.Key = d.Key 
WHERE 
  c.Country = 'Bulgaria' 
ORDER BY 
  Date ASC

-- COMMAND ----------

SELECT * FROM covid.cases

-- COMMAND ----------

SELECT * FROM covid.deaths

-- COMMAND ----------


