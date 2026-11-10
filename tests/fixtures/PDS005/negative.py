import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Turning the guard back on / leaving it off is fine.
ps.set_option("compute.ops_on_diff_frames", False)
ps.reset_option("compute.ops_on_diff_frames")
ps.options.compute.ops_on_diff_frames = False
with ps.option_context("compute.ops_on_diff_frames", False):
    pass
spark.conf.set("pandas_on_Spark.compute.ops_on_diff_frames", "false")

# Other pandas-on-Spark options.
ps.set_option("compute.default_index_type", "distributed-sequence")
ps.set_option("compute.max_rows", True)
with ps.option_context("compute.ops_on_diff_frames_x", True, "display.max_rows", 10):
    pass

# Plain pandas has no such guard: never reported.
pd.set_option("compute.ops_on_diff_frames", True)
with pd.option_context("compute.ops_on_diff_frames", True):
    pass
pd.options.compute.ops_on_diff_frames = True

# The pandas option name in the Spark conf without the pandas_on_Spark prefix is
# not read by pandas-on-Spark.
spark.conf.set("compute.ops_on_diff_frames", "true")

# Non-literal values: unknown.
flag = load_flag()  # noqa: F821
ps.set_option("compute.ops_on_diff_frames", flag)


# A user object with the same method names.
class Settings:
    def set_option(self, key, value):
        return key, value


Settings().set_option("compute.ops_on_diff_frames", True)
