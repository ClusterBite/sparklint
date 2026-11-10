import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("sales")

# Below the thresholds.
a = df.cube("a", "b", "c", "d").count()
b = df.rollup("a", "b", "c", "d", "e", "f", "g").count()
c = df.groupBy("a", "b", "c", "d", "e", "f").count()

# Column count unknown.
cols = ["a", "b", "c", "d", "e"]
d = df.cube(*cols).count()

# SQL below the threshold / explicit grouping sets / CUBE in a string elsewhere.
q1 = spark.sql("SELECT a FROM t GROUP BY CUBE(a, b, upper(concat(c, d, e, f)))")
q2 = spark.sql("SELECT a FROM t GROUP BY GROUPING SETS ((a, b), (a), ())")
q3 = spark.sql("SELECT 'cube(a, b, c, d, e)' AS s FROM t GROUP BY a")
q4 = spark.sql("SELECT a FROM t GROUP BY a, b, c, d WITH ROLLUP")
q5 = spark.sql("SELECT cube_id FROM cube WHERE x IN (1, 2, 3, 4, 5, 6)")


# Look-alikes: user classes, untyped params, pandas, Py4J handles.
class Olap:
    def cube(self, *dims):
        return dims


o = Olap().cube("a", "b", "c", "d", "e")


def untyped(frame):
    return frame.cube("a", "b", "c", "d", "e").count()


pdf = pd.DataFrame({"a": [1]})
p = pdf.groupby(["a", "b", "c", "d", "e"]).sum()


class Wrapper:
    def __init__(self, jdf):
        self._jdf = jdf

    def go(self):
        return self._jdf.cube("a", "b", "c", "d", "e")


not_spark = Olap()
not_spark.sql("SELECT a FROM t GROUP BY CUBE(a, b, c, d, e)")
