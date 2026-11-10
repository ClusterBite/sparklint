from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.sql.broadcastTimeout", "600")  # expect: CFG016
spark.conf.set("spark.sql.broadcastTimeout", 3600)  # expect: CFG016
spark.conf.set("SPARK.SQL.BROADCASTTIMEOUT", "36000")  # expect: CFG016
spark.conf.set("spark.sql.broadcastTimeout", "20min")  # expect: CFG016
spark.conf.set("spark.sql.broadcastTimeout", "2h")  # expect: CFG016
spark.conf.set("spark.sql.broadcastTimeout", -1)  # expect: CFG016
spark.sql("SET spark.sql.broadcastTimeout = 1200")  # expect: CFG016

KEY = "spark.sql.broadcastTimeout"
LONG = "7200"
spark.conf.set(KEY, LONG)  # expect: CFG016

session = (
    SparkSession.builder.appName("job")
    .config("spark.sql.broadcastTimeout", "900")  # expect: CFG016
    .getOrCreate()
)
session2 = SparkSession.builder.config(map={"spark.sql.broadcastTimeout": 1800}).getOrCreate()  # expect: CFG016
conf = SparkConf().set("spark.sql.broadcastTimeout", "1000")  # expect: CFG016


def slow_join(spark: SparkSession, a, b):
    spark.conf.set("spark.sql.broadcastTimeout", "3000")  # expect: CFG016
    out = a.join(b, "k")
    out.count()
    spark.conf.set("spark.sql.broadcastTimeout", "300")
