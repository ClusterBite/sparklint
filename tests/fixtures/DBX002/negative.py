from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

import pandas as pd

df = spark.table("t")
e = df.isEmpty()


class Holder:
    rdd = None


h = Holder()
x = h.rdd
pdf = pd.DataFrame()
