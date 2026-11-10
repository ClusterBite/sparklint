import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Default or lower values.
spark.conf.set("spark.sql.broadcastTimeout", "300")
spark.conf.set("spark.sql.broadcastTimeout", 120)
spark.conf.set("spark.sql.broadcastTimeout", "5min")
spark.conf.set("spark.sql.broadcastTimeout", "300s")

# Other broadcast / timeout keys.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "52428800")
spark.conf.set("spark.network.timeout", "800s")
spark.conf.set("spark.sql.execution.timeout", "3600000")

# Reading or unsetting the setting.
timeout = spark.conf.get("spark.sql.broadcastTimeout")
spark.conf.unset("spark.sql.broadcastTimeout")

# Values not known statically or not durations.
spark.conf.set("spark.sql.broadcastTimeout", os.environ["BCAST_TIMEOUT"])
spark.conf.set("spark.sql.broadcastTimeout", "1.5h")


def configure(spark: SparkSession, seconds):
    spark.conf.set("spark.sql.broadcastTimeout", seconds)


# Look-alikes.
settings = {"spark.sql.broadcastTimeout": "3600"}
spark.sql("SELECT 'spark.sql.broadcastTimeout = 3600' AS note")
spark.read.option("spark.sql.broadcastTimeout", "3600").parquet("/p")


class Cfg:
    def set(self, k, v):
        pass


Cfg().set("spark.sql.broadcastTimeout", "3600")

spark.conf.set("spark.sql.broadcastTimeout", "3600")  # noqa: CFG016
