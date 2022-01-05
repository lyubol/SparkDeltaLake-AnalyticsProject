# Databricks notebook source
# DBTITLE 1,Create Folders Structure
dbutils.fs.mkdirs("/FileStore/tables/Covid")

# Store inbound files
dbutils.fs.mkdirs("/FileStore/tables/Covid/00.Inbound")

# Bronze layer = store all files in original format
dbutils.fs.mkdirs("/FileStore/tables/Covid/01.Bronze")

# Silver layer - cleaning and transformations are applied
dbutils.fs.mkdirs("/FileStore/tables/Covid/02.Silver")

# Gold layer = Aggregations are applied
dbutils.fs.mkdirs("/FileStore/tables/Covid/03.Gold")

# Sub-folders for cases, deaths and countries datasets, "within Bronze"
dbutils.fs.mkdirs("/FileStore/tables/Covid/01.Bronze/Cases")
dbutils.fs.mkdirs("/FileStore/tables/Covid/01.Bronze/Deaths")
dbutils.fs.mkdirs("/FileStore/tables/Covid/01.Bronze/Countries")

# Sub-folders for cases, deaths and countries datasets, "within Silver"
dbutils.fs.mkdirs("/FileStore/tables/Covid/02.Silver/Cases")
dbutils.fs.mkdirs("/FileStore/tables/Covid/02.Silver/Deaths")
dbutils.fs.mkdirs("/FileStore/tables/Covid/02.Silver/Countries")

# Sub-folders for cases, deaths and countries datasets, "within Gold"
dbutils.fs.mkdirs("/FileStore/tables/Covid/03.Gold/Cases")
dbutils.fs.mkdirs("/FileStore/tables/Covid/03.Gold/Deaths")
dbutils.fs.mkdirs("/FileStore/tables/Covid/03.Gold/Countries")

# remove folders if needed
# dbutils.fs.rm("/FileStore/tables/Covid/Gold/Cases/", True)
# dbutils.fs.rm("/FileStore/tables/Covid/Gold/Deaths/", True)
# dbutils.fs.rm("/FileStore/tables/Covid/Gold/Countries/", True)

# dbutils.fs.rm("/FileStore/tables/Covid/Silver/Cases/", True)
# dbutils.fs.rm("/FileStore/tables/Covid/Silver/Deaths/", True)
# dbutils.fs.rm("/FileStore/tables/Covid/Silver/Countries/", True)

# dbutils.fs.rm("/FileStore/tables/Covid/Bronze/Cases/", True)
# dbutils.fs.rm("/FileStore/tables/Covid/Bronze/Deaths/", True)
# dbutils.fs.rm("/FileStore/tables/Covid/Bronze/Countries/", True)

dbutils.fs.rm("/FileStore/tables/Covid/Inbound/", True)
