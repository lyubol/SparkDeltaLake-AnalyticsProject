# Databricks notebook source
# MAGIC %md
# MAGIC # Data Cleaning and Transformations

# COMMAND ----------

# DBTITLE 1,Read 'Bronze' delta 
cases_df = spark.read.format("delta").load("/FileStore/tables/Covid/01.Bronze/Cases/")

display(cases_df)

# COMMAND ----------

# DBTITLE 1,Check schema
cases_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Remove sub-header
cases_df = cases_df.filter(cases_df.Country != "#country+name")

display(cases_df)

# COMMAND ----------

# DBTITLE 1,Check countries list
for country in cases_df.select(cases_df.Country).distinct().collect():
    print(country[0])

# COMMAND ----------

# DBTITLE 1,Rename and drop values from the 'Country' column
# replacing "Taiwan*" with "Taiwan"
cases_df = cases_df.replace("Taiwan*", "Taiwan", subset="Country")

# replacing "Czechia" with "Czech Republic"
cases_df = cases_df.replace("Czechia", "Czech Republic", subset="Country")

# replacing "US" with "United States"
cases_df = cases_df.replace("US", "United States", subset="Country")

# replacing "Fiji" with "Fiji Islands"
cases_df = cases_df.replace("Fiji", "Fiji Islands", subset="Country")

# replacing "Bahamas" with "Bahamas The"
cases_df = cases_df.replace("Bahamas", "Bahamas The", subset="Country")

# replacing "Cabo Verde" with "Cape Verde"
cases_df = cases_df.replace("Cabo Verde", "Cape Verde", subset="Country")

# replacing "Cote d'Ivoire" with "Cote D'Ivoire (Ivory Coast)"
cases_df = cases_df.replace("Cote d'Ivoire", "Cote D'Ivoire (Ivory Coast)", subset="Country")

# replacing "Korea, South" with "South Korea"
cases_df = cases_df.replace("Korea, South", "South Korea", subset="Country")

# drop values, which are not country names
cases_df = cases_df.where((cases_df["Country"] != "Summer Olympics 2020") & (cases_df["Country"] != "Diamond Princess") & (cases_df["Country"] != "MS Zaandam"))

# COMMAND ----------

# DBTITLE 1,Check count of distinct countries
# It looks like some countries appear more than others. E.g China has almost 25,000 records but only around 700 were expected.

cases_df.groupBy('Country').count().show(50)

# COMMAND ----------

# DBTITLE 1,Check why China has almost 25,000 records
# It looks like each of China's provinces contains data for 714 days. The data should be aggregated at the next stage.

cases_df.filter(cases_df.Country == 'China').groupBy('State').count().show()

# COMMAND ----------

# DBTITLE 1,Write to 'Silver'
cases_df.write.format("delta").mode("overwrite").save("/FileStore/tables/Covid/02.Silver/Cases")

# check
dbutils.fs.ls("/FileStore/tables/Covid/02.Silver/Cases/")
