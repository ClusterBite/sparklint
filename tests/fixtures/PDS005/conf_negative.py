from pyspark import SparkConf
from pyspark.sql import SparkSession

# Off, or not a literal: never reported.
spark = (
    SparkSession.builder.config(
        map={"pandas_on_Spark.compute.ops_on_diff_frames": "false"}
    )
    .getOrCreate()
)
SparkConf().set("pandas_on_Spark.compute.ops_on_diff_frames", "false")
spark.sql("SET pandas_on_Spark.compute.ops_on_diff_frames = false")
spark.conf.unset("pandas_on_Spark.compute.ops_on_diff_frames")
spark.conf.set("pandas_on_Spark.compute.ops_on_diff_frames", load_flag())  # noqa: F821

# A different option under the same prefix, and the key inside a string literal.
spark.conf.set("pandas_on_Spark.compute.max_rows", "true")
spark.sql("SELECT 'SET pandas_on_Spark.compute.ops_on_diff_frames = true' AS note")
