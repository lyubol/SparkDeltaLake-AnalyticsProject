# Databricks notebook source
# MAGIC %md
# MAGIC # Data aggregations

# COMMAND ----------

# DBTITLE 1,Read 'Silver' delta
cases_df = spark.read.format("delta").load("/FileStore/tables/Covid/02.Silver/Cases/")

display(cases_df)

# COMMAND ----------

# DBTITLE 1,Aggregate cases by date and country
from pyspark.sql.functions import col, sha2, concat

cases_agg_df = (cases_df.groupBy("Date", "Country").agg({"Value":"sum"})
                        .withColumn("CaseKey", sha2(concat(col("Country"), col("Date")), 256))
                        .select(col("CaseKey").alias("CaseKey"),
                                col("Date").alias("Date"),
                                col("Country").alias("Country"),
                                col("sum(Value)").alias("Value"))
               )

display(cases_agg_df)

# COMMAND ----------

# DBTITLE 1,Read 'Silver' countries delta 
countries_df = spark.read.format("delta").load("/FileStore/tables/Covid/02.Silver/Countries/")

display(countries_df)

# COMMAND ----------

# DBTITLE 1,Join with the countries DF to bring the 'IsoCode' column
cases_agg_joined_df = (cases_agg_df.join(countries_df, cases_agg_df.Country == countries_df.Country, how='left')
                                  .select(cases_agg_df['*'], countries_df.IsoCode3)
                      )

display(cases_agg_joined_df)

# COMMAND ----------

# DBTITLE 1,Write to 'Gold' 
cases_agg_joined_df.write.format("delta").mode("overwrite").save("/FileStore/tables/Covid/03.Gold/Cases/")
