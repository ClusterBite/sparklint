# Databricks notebook source
# MAGIC %md
# MAGIC # Bare magics next to `# MAGIC` cells (hand-edited / old exports)

# COMMAND ----------

%pip install --quiet databricks-genai==1.1.7 mlflow==2.19.0 \
    databricks-sdk==0.40.0
%restart_python

# COMMAND ----------

!/databricks/python3/bin/python -m pip install --upgrade pip
!pip install faker colorama

# COMMAND ----------

%run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,Shell cell: the whole cell is not Python
%sh
ls -l /dbfs/tmp | wc -l
if [ -d /tmp/x ]; then rm -rf /tmp/x; fi

# COMMAND ----------

%sql
SELECT * FROM events WHERE note = 'it''s'
LIMIT 10

# COMMAND ----------

%scala
val x = spark.table("events")

# COMMAND ----------

df = spark.table("events")
files = !ls /tmp
if files:
    %pip install colorama
rows = df.collect()  # expect: ACT002
ratio = (len(rows)
         % 7)
query = """
%sql is only text here
!so is this
"""

# COMMAND ----------

%python
more = df.collect()  # expect: ACT002
few = df.limit(3).collect()
