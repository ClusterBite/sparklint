# Databricks notebook source
import pandas as pd

big = spark.table("events")
local = spark.createDataFrame(pd.DataFrame({"id": [1, 2]}))
tables = spark.sql("SHOW TABLES IN sales")

# COMMAND ----------

# Frames that fit the driver are a deliberate small preview.
display(local)
display(tables)
display(local.join(big, "id"))  # expect: ACT010
local.show()
tables.show()
local.join(big, "id").show()  # expect: ACT010
