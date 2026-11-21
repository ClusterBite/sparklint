from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


# The loop returns after its first iteration: the withColumn chain runs once.
def first_campaign(df, starts):
    for s in starts:
        df = df.withColumn("a", F.lit(s))
        df = df.withColumn("b", F.lit(s))
        return df.withColumn("c", F.lit(s))
    return df


# A repeating loop is still reported.
def every_campaign(df, starts):
    for s in starts:
        df = df.withColumn("a", F.lit(s))  # expect: PRF001
    return df
