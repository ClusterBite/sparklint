from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Enabling, or other AQE keys.
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "false")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
spark.sql("SET spark.sql.adaptive.enabled = true")
spark.sql("SELECT 'spark.sql.adaptive.enabled=false' AS note")

# Reading, not writing.
enabled = spark.conf.get("spark.sql.adaptive.enabled", "false")

# Values that are not constant.
def configure(flag):
    spark.conf.set("spark.sql.adaptive.enabled", flag)


key = f"spark.sql.adaptive.{'enabled'}"
spark.conf.set(f"spark.sql.adaptive.{name}", "false")


# Look-alikes: not a Spark config.
class Settings:
    def set(self, k, v):
        pass


s = Settings()
s.set("spark.sql.adaptive.enabled", "false")
d = {}
d["spark.sql.adaptive.enabled"] = "false"
os_env = dict(spark_sql_adaptive_enabled="false")
cfg = SparkConf().get("spark.sql.adaptive.enabled")

spark.conf.set("spark.sql.adaptive.enabled", "false")  # noqa: CFG001
