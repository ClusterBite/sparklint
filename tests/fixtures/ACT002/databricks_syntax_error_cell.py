# Databricks notebook source
df = spark.table("events")

# COMMAND ----------

# A real typo: this cell is reported (SYN001) and skipped, the others are linted.
token_count = len(tokenizer.encode(text))y
lost = df.collect()

# COMMAND ----------

rows = df.collect()  # expect: ACT002

# COMMAND ----------

image = spark.sql('select `image`.`/Volumes/xx/xx/generated_images`)
also_lost = df.collect()

# COMMAND ----------

more = df.collect()  # expect: ACT002
