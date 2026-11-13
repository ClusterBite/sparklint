import pandas as pd
from pyspark.sql import SparkSession

from helpers import fetch

spark = SparkSession.builder.getOrCreate()

# A `*_df` name used with pandas-only attributes is not guessed a Spark DataFrame.
result_df = fetch()
print(result_df.shape)
print(result_df.count())

scores_df = fetch()
top = scores_df.sort_values("score").iloc[:10]
if scores_df.count() > 0:
    print(top)

# No pandas evidence: the name still marks a DataFrame.
other_df = fetch()
print(other_df.count())  # expect: ACT007
