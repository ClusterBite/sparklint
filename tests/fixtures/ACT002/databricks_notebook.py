# Databricks notebook source
# MAGIC %md
# MAGIC # Exploration

# COMMAND ----------

df = spark.table("events")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM events

# COMMAND ----------

rows = df.collect()  # expect: ACT002
small = df.limit(10).collect()
