# Databricks notebook source
# MAGIC %md
# MAGIC # Create schema and read raw csv file

# COMMAND ----------

# DBTITLE 1,Create schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

countries_schema = StructType(fields = [
    StructField("Id", IntegerType(), True),
    StructField("Country", StringType(), True),
    StructField("IsoCode3", StringType(), True),
    StructField("IsoCode2", StringType(), True),
    StructField("NumericCode", IntegerType(), True),
    StructField("PhoneCode", IntegerType(), True),
    StructField("Capital", StringType(), True),
    StructField("Currency", StringType(), True),
    StructField("CurrencyName", StringType(), True),
    StructField("CurrencySymbol", StringType(), True),
    StructField("Tld", StringType(), True),
    StructField("Native", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Subregion", StringType(), True),
    StructField("Timezones", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Emoji", StringType(), True),
    StructField("EmojiU", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Read csv file
countries_df = (spark.read
                    .format("csv")
                    .schema(countries_schema)
                    .option("header", True)
                    .load("/FileStore/tables/Covid/00.Inbound/countries.csv")
               )

display(countries_df)

# COMMAND ----------

# DBTITLE 1,Write to 'Bronze' 
countries_df.write.format("delta").mode("overwrite").save("/FileStore/tables/Covid/01.Bronze/Countries/")

# check
dbutils.fs.ls("/FileStore/tables/Covid/01.Bronze/Countries/")
