# Databricks notebook source
# DBTITLE 1,Read 'Bronze' delta
countries_df = (spark.read
                    .format("delta")
                    .load("/FileStore/tables/Covid/01.Bronze/Countries/")
                    .select("Id", "Country", "IsoCode3", "NumericCode", "Capital", "Region", "Subregion", "Latitude", "Longitude")
               )

display(countries_df)

# COMMAND ----------

# DBTITLE 1,Write to 'Bronze' 
countries_df.write.format("delta").mode("overwrite").save("/FileStore/tables/Covid/02.Silver/Countries/")

# check
dbutils.fs.ls("/FileStore/tables/Covid/02.Silver/Countries/")
