import random

import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")

# Sampling instead of sorting.
a = df.sample(fraction=0.01).limit(100)
b = df.where(F.rand() < 0.01)
c = df.withColumn("r", F.rand()).where("r < 0.1").orderBy("id")

# A random key that is not the leading sort key (a tie-breaker).
d = df.orderBy("day", F.rand())

# Per-group sampling: a partitioned window, not a global sort.
w = Window.partitionBy("user_id").orderBy(F.rand())
e = df.withColumn("rn", F.row_number().over(w)).where("rn <= 3")

# Per-partition shuffles are not global sorts.
f = df.sortWithinPartitions(F.rand())
g = spark.sql("SELECT * FROM events DISTRIBUTE BY rand()")
h = spark.sql("SELECT * FROM events SORT BY rand()")

# SQL window specs, strings and comments.
i = spark.sql("SELECT *, row_number() OVER (PARTITION BY k ORDER BY rand()) AS rn FROM t")
j = spark.sql("SELECT 'ORDER BY rand()' AS note FROM t ORDER BY id")
k = spark.sql("SELECT a FROM t -- ORDER BY rand()\n")
m = spark.sql("SELECT * FROM t ORDER BY random_score DESC")

# Not Spark: pandas, plain Python, user classes.
pdf = pd.DataFrame({"x": [3, 1, 2]})
n = pdf.sort_values("x")
items = [3, 1, 2]
items.sort(key=lambda _: random.random())


class Deck:
    def sort(self, key):
        return key


Deck().sort(F.rand())

df.orderBy(F.rand()).limit(10)  # noqa: PRF027
