import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Enabling, or the size/file thresholds of dynamic file pruning.
spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", True)
spark.conf.set("spark.databricks.optimizer.deltaTableSizeThreshold", "1000000000")
spark.conf.set("spark.databricks.optimizer.deltaTableFilesThreshold", "5")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly", "false")

# Reading the setting.
dfp = spark.conf.get("spark.databricks.optimizer.dynamicFilePruning")

# Value not known statically.
spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", os.environ["DFP"])

# Look-alikes.
settings = {"spark.databricks.optimizer.dynamicFilePruning": "false"}
spark.sql("SELECT 'spark.databricks.optimizer.dynamicFilePruning=false' AS note")


class Cfg:
    def set(self, k, v):
        pass


Cfg().set("spark.databricks.optimizer.dynamicFilePruning", "false")

spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "false")  # noqa: CFG015
