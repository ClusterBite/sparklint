import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Grouped pandas-on-Spark ops partition by the key: fine.
psdf = ps.read_table("sales")
a = psdf.groupby("user")["amount"].cumsum()
b = psdf.groupby("user").shift(1)
c = psdf.groupby(["user", "day"])["amount"].rank()
g = psdf.groupby("user")
d = g["amount"].diff()
e = g.cummax()

# Rolling / expanding objects are different types.
r = psdf["amount"].rolling(3).sum()
x = psdf["amount"].expanding().max()

# Reductions and positional lookups are not frames.
total = psdf["amount"].sum()
first = psdf["amount"][0]

# Plain pandas: runs locally, no Spark window.
pdf = pd.DataFrame({"a": [1, 2, 3]})
p1 = pdf["a"].cumsum()
p2 = pdf.shift(1)
p3 = pdf.rank()
p4 = pd.Series([1, 2]).pct_change()
local = psdf.to_pandas()
p5 = local["amount"].diff()

# Spark DataFrames / Columns: explicit windows are PRF005's business.
sdf = spark.table("sales")
w = Window.partitionBy("user").orderBy("ts")
s1 = sdf.withColumn("r", F.rank().over(w))
s2 = sdf.withColumn("lag", F.lag("amount").over(w))


# Unknown receivers are not reported.
def untyped(frame):
    return frame["amount"].cumsum()


# A user class with the same method names.
class Ledger:
    def cumsum(self):
        return 0

    def shift(self, n):
        return n


ledger = Ledger()
ledger.cumsum()
ledger.shift(1)
