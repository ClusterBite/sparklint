# Databricks notebook source
# The notebook globals behind a fallback: not reported.
try:
    sc.setJobGroup("etl", "nightly")
except NameError:
    pass

# COMMAND ----------

try:
    tables = sqlContext.tableNames()
except Exception:
    tables = [t.name for t in spark.catalog.listTables()]

# COMMAND ----------

try:
    n = sc.defaultParallelism  # expect: DBX001
except AttributeError:
    n = 8
