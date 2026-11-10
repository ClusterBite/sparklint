import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Enabling, other values, other keys.
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "auto")
spark.conf.set("spark.databricks.delta.autoCompact.maxFileSize", "128m")
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", "false")
spark.conf.get("spark.databricks.delta.optimizeWrite.enabled", "false")
spark.sql("SET spark.databricks.delta.optimizeWrite.enabled = true")

# Non-constant values.
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", os.environ["OW"])

# Table properties that enable compaction, or other properties set false.
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')")
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.appendOnly' = 'false')")
spark.sql("SHOW TBLPROPERTIES t")
spark.sql("SELECT 'delta.autoOptimize.optimizeWrite = false' AS note")

# Not a SparkSession.
class Db:
    def sql(self, q):
        pass

    def set(self, k, v):
        pass


Db().sql("ALTER TABLE t SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false')")
Db().set("spark.databricks.delta.optimizeWrite.enabled", "false")
settings = {"spark.databricks.delta.autoCompact.enabled": "false"}

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")  # noqa: CFG009
