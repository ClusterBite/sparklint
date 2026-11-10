# Databricks notebook source
df = spark.table("events")

# COMMAND ----------

# The last expression of a cell is displayed by the notebook: used, not discarded.
df.head()

# COMMAND ----------

df.take(5)

# COMMAND ----------

df.count()  # expect: ACT007
df.first()

# COMMAND ----------

df.collect()  # expect: ACT007
x = 1

# COMMAND ----------

if x:
    df.count()  # expect: ACT007

# COMMAND ----------

def probe(sdf):
    sdf.count()  # expect: ACT007


probe(df)

# COMMAND ----------

%pip install faker
df.toPandas()

# COMMAND ----------

# Computed only to be logged: still a full job for a log line.
print(f"rows: {df.count()}")  # expect: ACT007

# COMMAND ----------

df.count()
