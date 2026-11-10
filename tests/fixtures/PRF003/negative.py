from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

a = spark.table("a")
b = spark.table("b")
c = a.join(b, on="id")
d = a.join(b, a.id == b.id)
e = a.join(b, ["id"], "left")
cols = ["a", "b", "c"]
s = ",".join(cols)
items = get_items()
r = items.join(other)
import os
p = os.path.join("a", "b")
