import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Enabling, or other compression settings.
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", True)
spark.conf.set("spark.io.compression.codec", "zstd")
spark.conf.set("spark.rdd.compress", "false")
spark.conf.set("spark.checkpoint.compress", "true")
spark.conf.set("spark.sql.parquet.compression.codec", "uncompressed")
spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "20000")

# Core (launch-time) keys written on a running session have no effect here
# (rejected or ignored by Spark), so the value is not reported.
spark.conf.set("spark.shuffle.compress", "false")
spark.sql("SET spark.broadcast.compress = false")

# Reading or unsetting the setting.
on = spark.conf.get("spark.shuffle.compress")
spark.conf.unset("spark.shuffle.compress")

# Value not known statically.
spark.conf.set("spark.shuffle.compress", os.environ["SHUFFLE_COMPRESS"])

# Look-alikes.
settings = {"spark.shuffle.compress": "false"}
spark.sql("SELECT 'spark.shuffle.compress = false' AS note")
spark.read.option("spark.broadcast.compress", "false").parquet("/p")
df = spark.read.parquet("/p")
df.write.option("compression", "none").parquet("/out")


class Cfg:
    def set(self, k, v):
        pass


Cfg().set("spark.shuffle.compress", "false")

spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "false")  # noqa: CFG018
