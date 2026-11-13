from pyspark import SparkConf
from pyspark.sql import SparkSession

# The Spark-conf form of the option, written every way Spark config is written.
spark = (
    SparkSession.builder.config(  # expect: PDS005
        map={"pandas_on_Spark.compute.ops_on_diff_frames": "true"}
    )
    .getOrCreate()
)

conf = SparkConf().set("pandas_on_Spark.compute.ops_on_diff_frames", "true")  # expect: PDS005

KEY = "pandas_on_Spark.compute.ops_on_diff_frames"
spark.conf.set(KEY, "TRUE")  # expect: PDS005

spark.sql("SET pandas_on_Spark.compute.ops_on_diff_frames = true")  # expect: PDS005
