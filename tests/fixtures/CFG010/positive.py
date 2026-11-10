from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.databricks.io.cache.enabled", "false")  # expect: CFG010
spark.conf.set("spark.databricks.io.cache.enabled", False)  # expect: CFG010
spark.conf.set("SPARK.DATABRICKS.IO.CACHE.ENABLED", "FALSE")  # expect: CFG010
spark.sql("SET spark.databricks.io.cache.enabled = false")  # expect: CFG010

KEY = "spark.databricks.io.cache.enabled"
spark.conf.set(KEY, "false")  # expect: CFG010

session = (
    SparkSession.builder.appName("job")
    .config("spark.databricks.io.cache.enabled", "false")  # expect: CFG010
    .getOrCreate()
)
session2 = SparkSession.builder.config(map={"spark.databricks.io.cache.enabled": False}).getOrCreate()  # expect: CFG010
conf = SparkConf().set("spark.databricks.io.cache.enabled", "false")  # expect: CFG010


def cold_read(spark: SparkSession, path):
    spark.conf.set("spark.databricks.io.cache.enabled", "false")  # expect: CFG010
    df = spark.read.parquet(path)
    df.count()
    spark.conf.set("spark.databricks.io.cache.enabled", "true")
