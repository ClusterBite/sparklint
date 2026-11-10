from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.sql.adaptive.enabled", "false")  # expect: CFG001
spark.conf.set("spark.sql.adaptive.enabled", False)  # expect: CFG001
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "False")  # expect: CFG001
spark.conf.set(key="spark.sql.adaptive.skewJoin.enabled", value="false")  # expect: CFG001
spark.sql("SET spark.sql.adaptive.enabled = false")  # expect: CFG001
spark.sql("set spark.sql.adaptive.skewJoin.enabled=false;")  # expect: CFG001

AQE = "spark.sql.adaptive.enabled"
OFF = "false"
spark.conf.set(AQE, OFF)  # expect: CFG001

session = (
    SparkSession.builder.appName("job")
    .config("spark.sql.adaptive.enabled", "false")  # expect: CFG001
    .getOrCreate()
)
session2 = SparkSession.builder.config(map={"spark.sql.adaptive.enabled": False}).getOrCreate()  # expect: CFG001

conf = SparkConf().setAppName("job").set("spark.sql.adaptive.enabled", "false")  # expect: CFG001
conf2 = SparkConf()
conf2.set("spark.sql.adaptive.coalescePartitions.enabled", "false")  # expect: CFG001


def tune(spark: SparkSession):
    spark.conf.set("spark.sql.adaptive.enabled", "false")  # expect: CFG001


# Set back later in the file: still reported (as info).
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")  # expect: CFG001
spark.table("t").join(spark.table("u"), "k").write.saveAsTable("out")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
