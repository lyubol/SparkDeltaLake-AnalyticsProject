# Databricks notebook source
# MAGIC %md
# MAGIC # Create schema and read raw csv file

# COMMAND ----------

# DBTITLE 1,Create schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType

cases_schema = StructType(fields = [
    StructField("State", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Lat", DoubleType(), True),
    StructField("Long", DoubleType(), True),
    StructField("Date", DateType(), True),
    StructField("Value", IntegerType(), True),
    StructField("IsoCode", StringType(), True),
    StructField("RegionCode", IntegerType(), True),
    StructField("SubRegionCode", IntegerType(), True),
    StructField("IntermediateRegionCode", IntegerType(), True)
])

# COMMAND ----------

# DBTITLE 1,Read csv file
cases_df = (spark.read
                .format("csv")
                .option("header", True)
                .schema(cases_schema)
                .load("/FileStore/tables/Covid/00.Inbound/time_series_covid19_confirmed_global_narrow.csv"))

display(cases_df)

# COMMAND ----------

# DBTITLE 1,Write to 'Bronze'
cases_df.write.format("delta").mode("overwrite").save("/FileStore/tables/Covid/01.Bronze/Cases/")

# check 
dbutils.fs.ls("/FileStore/tables/Covid/01.Bronze/Cases/")
