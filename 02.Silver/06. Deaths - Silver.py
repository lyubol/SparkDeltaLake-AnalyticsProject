# Databricks notebook source
# MAGIC %md
# MAGIC # Data Cleaning and Transformations

# COMMAND ----------

# DBTITLE 1,Read 'Bronze' delta 
deaths_df = spark.read.format("delta").load("/FileStore/tables/Covid/01.Bronze/Deaths/")

display(deaths_df)

# COMMAND ----------

# DBTITLE 1,Check schema
deaths_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Remove sub-header
deaths_df = deaths_df.filter(deaths_df.Country != "#country+name")

display(deaths_df)

# COMMAND ----------

# DBTITLE 1,Check countries list
for country in deaths_df.select(deaths_df.Country).distinct().collect():
    print(country[0])

# COMMAND ----------

# DBTITLE 1,Rename and drop values from the 'Country' column
# replacing "Taiwan*" with "Taiwan"
deaths_df = deaths_df.replace("Taiwan*", "Taiwan", subset="Country")

# replacing "Czechia" with "Czech Republic"
deaths_df = deaths_df.replace("Czechia", "Czech Republic", subset="Country")

# replacing "US" with "United States"
deaths_df = deaths_df.replace("US", "United States", subset="Country")

# replacing "Fiji" with "Fiji Islands"
deaths_df = deaths_df.replace("Fiji", "Fiji Islands", subset="Country")

# replacing "Bahamas" with "Bahamas The"
deaths_df = deaths_df.replace("Bahamas", "Bahamas The", subset="Country")

# replacing "Cabo Verde" with "Cape Verde"
deaths_df = deaths_df.replace("Cabo Verde", "Cape Verde", subset="Country")

# replacing "Cote d'Ivoire" with "Cote D'Ivoire (Ivory Coast)"
deaths_df = deaths_df.replace("Cote d'Ivoire", "Cote D'Ivoire (Ivory Coast)", subset="Country")

# replacing "Korea, South" with "South Korea"
deaths_df = deaths_df.replace("Korea, South", "South Korea", subset="Country")

# drop values, which are not country names
deaths_df = deaths_df.where((deaths_df["Country"] != "Summer Olympics 2020") & (deaths_df["Country"] != "Diamond Princess") & (deaths_df["Country"] != "MS Zaandam"))

# COMMAND ----------

# DBTITLE 1,Check count of distinct countries
# It looks like some countries appear more than others. E.g China has almost 25,000 records but only around 700 were expected.

deaths_df.groupBy('Country').count().show(50)

# COMMAND ----------

# DBTITLE 1,Check why China has almost 25,000 records
# It looks like each of China's provinces contains data for 714 days. The data should be aggregated at the next stage.

deaths_df.filter(deaths_df.Country == 'China').groupBy('State').count().show()

# COMMAND ----------

# DBTITLE 1,Write to 'Silver'
deaths_df.write.format("delta").mode("overwrite").save("/FileStore/tables/Covid/02.Silver/Deaths/")

# check
dbutils.fs.ls("/FileStore/tables/Covid/02.Silver/Deaths/")
