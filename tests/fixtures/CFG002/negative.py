from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Positive thresholds (too-high ones are CFG007, not CFG002).
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "4g")
spark.conf.set("spark.sql.broadcastTimeout", "-1")

# Not constant / not a write.
def configure(t):
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", t)


current = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")


class Cfg:
    def set(self, k, v):
        pass


Cfg().set("spark.sql.autoBroadcastJoinThreshold", -1)
opts = {"spark.sql.autoBroadcastJoinThreshold": -1}

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # noqa: CFG002
