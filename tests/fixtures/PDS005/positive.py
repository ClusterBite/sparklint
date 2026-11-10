import pyspark.pandas as ps
from pyspark.pandas import option_context, set_option
from pyspark.pandas.config import set_option as cfg_set_option
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

ps.set_option("compute.ops_on_diff_frames", True)  # expect: PDS005
ps.set_option("compute.ops_on_diff_frames", value=True)  # expect: PDS005
set_option(key="compute.ops_on_diff_frames", value=True)  # expect: PDS005
cfg_set_option("compute.ops_on_diff_frames", True)  # expect: PDS005

with ps.option_context("compute.ops_on_diff_frames", True):  # expect: PDS005
    pass
with option_context("display.max_rows", 10, "compute.ops_on_diff_frames", True):  # expect: PDS005
    pass

ps.options.compute.ops_on_diff_frames = True  # expect: PDS005

spark.conf.set("pandas_on_Spark.compute.ops_on_diff_frames", "true")  # expect: PDS005
spark.conf.set("pandas_on_Spark.compute.ops_on_diff_frames", True)  # expect: PDS005

session = (
    SparkSession.builder.config("pandas_on_Spark.compute.ops_on_diff_frames", "true")  # expect: PDS005
    .getOrCreate()
)
