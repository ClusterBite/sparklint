from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

HEAP = "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"

# Session setting, any spelling of the key, full or short class name.
spark.conf.set(  # expect: STM011
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
)
spark.conf.set("SPARK.SQL.STREAMING.STATESTORE.PROVIDERCLASS", HEAP)  # expect: STM011
spark.sql("SET spark.sql.streaming.stateStore.providerClass = org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")  # expect: STM011

KEY = "spark.sql.streaming.stateStore.providerClass"
spark.conf.set(KEY, HEAP)  # expect: STM011

# Builder and SparkConf.
session = (
    SparkSession.builder.appName("stream")
    .config("spark.sql.streaming.stateStore.providerClass", HEAP)  # expect: STM011
    .getOrCreate()
)
session2 = SparkSession.builder.config(map={KEY: HEAP}).getOrCreate()  # expect: STM011
conf = SparkConf().set(KEY, "HDFSBackedStateStoreProvider")  # expect: STM011
conf2 = SparkConf().setAll([(KEY, HEAP)])  # expect: STM011


def start(spark: SparkSession, df):
    spark.conf.set(KEY, HEAP)  # expect: STM011
    return (
        df.groupBy("k")
        .count()
        .writeStream.outputMode("update")
        .option("checkpointLocation", "/chk")
        .start()
    )
