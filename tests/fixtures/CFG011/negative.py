import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# In the normal band.
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 5000)
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "2000")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 50000)
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "500")

# Other Arrow settings.
spark.conf.set("spark.sql.execution.arrow.maxBytesPerBatch", "1000000000")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Reading, or values not known statically.
n = spark.conf.get("spark.sql.execution.arrow.maxRecordsPerBatch")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", os.environ["ARROW_BATCH"])
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10k")

# Look-alikes.
opts = {"spark.sql.execution.arrow.maxRecordsPerBatch": "1000000"}


class Cfg:
    def set(self, k, v):
        pass


Cfg().set("spark.sql.execution.arrow.maxRecordsPerBatch", "1000000")

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1000000")  # noqa: CFG011
