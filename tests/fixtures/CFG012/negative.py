import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# In the normal band.
spark.conf.set("spark.sql.files.maxPartitionBytes", "128m")
spark.conf.set("spark.sql.files.maxPartitionBytes", 134217728)
spark.conf.set("spark.sql.files.maxPartitionBytes", "256MB")
spark.conf.set("spark.sql.files.maxPartitionBytes", "1g")
spark.conf.set("spark.sql.files.maxPartitionBytes", "8m")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "2g")
spark.conf.set("spark.sql.files.openCostInBytes", "4194304")
spark.conf.set("spark.sql.files.openCostInBytes", "1m")

# Other size settings.
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1m")
spark.conf.set("spark.sql.files.maxRecordsPerFile", 1000)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "16g")

# Reading, or values not known statically.
v = spark.conf.get("spark.sql.files.maxPartitionBytes")
spark.conf.set("spark.sql.files.maxPartitionBytes", os.environ["MPB"])
spark.conf.set("spark.sql.files.maxPartitionBytes", "1.5g")
spark.conf.unset("spark.sql.files.maxPartitionBytes")

# Look-alikes.
opts = {"spark.sql.files.maxPartitionBytes": "4g"}
df = spark.read.option("maxPartitionBytes", "4g").parquet("/p")
spark.sql("SELECT 'spark.sql.files.maxPartitionBytes = 4g'")


class Cfg:
    def set(self, k, v):
        pass


Cfg().set("spark.sql.files.maxPartitionBytes", "4g")

spark.conf.set("spark.sql.files.maxPartitionBytes", "4g")  # noqa: CFG012
