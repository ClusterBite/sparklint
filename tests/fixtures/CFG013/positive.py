from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "400")  # expect: CFG013
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", 2000)  # expect: CFG013
spark.conf.set("SPARK.SQL.ADAPTIVE.COALESCEPARTITIONS.MINPARTITIONNUM", "1")  # expect: CFG013
spark.conf.set("spark.sql.adaptive.minNumPostShufflePartitions", "200")  # expect: CFG013
spark.sql("SET spark.sql.adaptive.coalescePartitions.minPartitionNum = 800")  # expect: CFG013

KEY = "spark.sql.adaptive.coalescePartitions.minPartitionNum"
spark.conf.set(KEY, "400")  # expect: CFG013

session = (
    SparkSession.builder.appName("job")
    .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "400")  # expect: CFG013
    .getOrCreate()
)
session2 = SparkSession.builder.config(  # expect: CFG013
    map={"spark.sql.adaptive.coalescePartitions.minPartitionNum": "100"}
).getOrCreate()
conf = SparkConf().set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "64")  # expect: CFG013


def run(spark: SparkSession, df):
    spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "400")  # expect: CFG013
    out = df.groupBy("k").count()
    spark.conf.unset("spark.sql.adaptive.coalescePartitions.minPartitionNum")
    return out
