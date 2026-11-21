from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
a = df.filter((F.col("a") > 1) & (F.col("b") < 2))
flags = 3
ok = flags & 1 == 1
mask = (flags & 2) > 0
b = F.col("a") > (1 & flags)
# One comparison against `x | y` of plain values: the integer-flag idiom, compares
# the Column with the combined flags as written.
READ, WRITE = 1, 2
c = df.filter(F.col("mode") == READ | WRITE)
d = df.filter(F.col("a") > 1 & 2)
n = len(df.columns) & 1 == 0
# A deliberate grouping with a comment before the closing parenthesis (on its own line).
g = F.col("a") > (F.col("x") & flags  # compare against the masked value
)
h = F.col("a") > (  # masked
    F.col("x") | flags)
