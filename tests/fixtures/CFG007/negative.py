from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Under the default 1 GB limit.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "512m")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1023mb")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# Unparseable / non-constant / other keys.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1.5g")
spark.conf.set("spark.sql.broadcastTimeout", "36000")
spark.conf.set("spark.sql.files.maxPartitionBytes", "2g")
spark.conf.set("spark.driver.maxResultSize", "16g")


def tune(n):
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", n * 1024**3)


limit = spark.conf.get("spark.sql.autoBroadcastJoinThreshold", "2g")


class Cfg:
    def set(self, k, v):
        pass


Cfg().set("spark.sql.autoBroadcastJoinThreshold", "2g")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "2g")  # noqa: CFG007
