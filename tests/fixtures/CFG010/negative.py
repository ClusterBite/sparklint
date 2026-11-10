import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Enabling, or other disk-cache settings.
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.databricks.io.cache.enabled", True)
spark.conf.set("spark.databricks.io.cache.maxDiskUsage", "50g")
spark.conf.set("spark.databricks.io.cache.compression.enabled", "false")

# Reading the setting.
enabled = spark.conf.get("spark.databricks.io.cache.enabled")

# Value not known statically.
spark.conf.set("spark.databricks.io.cache.enabled", os.environ["DISK_CACHE"])

# Look-alikes.
settings = {"spark.databricks.io.cache.enabled": "false"}
spark.sql("SELECT 'spark.databricks.io.cache.enabled = false' AS note")


class Cfg:
    def set(self, k, v):
        pass


Cfg().set("spark.databricks.io.cache.enabled", "false")

spark.conf.set("spark.databricks.io.cache.enabled", "false")  # noqa: CFG010
