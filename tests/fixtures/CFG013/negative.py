from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Related AQE settings that are fine to tune.
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1m")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "1000")
spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")

# Reading or unsetting.
n = spark.conf.get("spark.sql.adaptive.coalescePartitions.minPartitionNum")
spark.conf.unset("spark.sql.adaptive.coalescePartitions.minPartitionNum")

# Look-alikes.
opts = {"spark.sql.adaptive.coalescePartitions.minPartitionNum": "400"}
spark.sql("SELECT 'spark.sql.adaptive.coalescePartitions.minPartitionNum = 400'")


class Cfg:
    def set(self, k, v):
        pass


Cfg().set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "400")

spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "400")  # noqa: CFG013
