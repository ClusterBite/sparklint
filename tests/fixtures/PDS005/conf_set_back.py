import pyspark.pandas as ps
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Turned on for one step and back off later: still reported, at info.
spark.conf.set("pandas_on_Spark.compute.ops_on_diff_frames", "true")  # expect: PDS005
psdf = ps.read_table("a")
spark.conf.set("pandas_on_Spark.compute.ops_on_diff_frames", "false")
