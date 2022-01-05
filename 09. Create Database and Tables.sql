-- Databricks notebook source
-- DBTITLE 1,Create database
DROP DATABASE IF EXISTS covid;

CREATE DATABASE covid;

-- COMMAND ----------

-- DBTITLE 1,Create 'cases' table
DROP TABLE IF EXISTS covid.cases;

CREATE TABLE covid.cases
USING DELTA
OPTIONS(path "/FileStore/tables/Covid/03.Gold/Cases/");

-- COMMAND ----------

SELECT * FROM covid.cases;

-- COMMAND ----------

-- DBTITLE 1,Create 'deaths' table
DROP TABLE IF EXISTS covid.deaths;

CREATE TABLE covid.deaths
USING DELTA
OPTIONS (path "/FileStore/tables/Covid/03.Gold/Deaths/")

-- COMMAND ----------

SELECT * FROM covid.deaths

-- COMMAND ----------

-- DBTITLE 1,Create 'countries' table
DROP TABLE IF EXISTS covid.countries;

CREATE TABLE covid.countries
USING DELTA 
OPTIONS (path "/FileStore/tables/Covid/02.Silver/Countries/")

-- COMMAND ----------

SELECT * FROM covid.countries

-- COMMAND ----------

SELECT * FROM covid.cases WHERE Country = 'Bulgaria' ORDER BY Date ASC

-- COMMAND ----------


