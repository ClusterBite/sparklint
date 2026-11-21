from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
a = df.filter(F.col("a") > 1 & F.col("b") < 2)  # expect: COR003[error] help~"(F.col"
b = df.filter(F.col("a") == 1 | F.col("b") == 2)  # expect: COR003
c = df.where(df.x >= 0 & df.y <= 5 & df.z == 3)  # expect: COR003
d = df.filter(col("a") > 1 & col("b") < 2)  # expect: COR003
e = df.filter(df["a"] > 1 & df["b"] < 2)  # expect: COR003
f = df.filter(F.col("a").isin(1, 2) & F.col("b") == 3)  # expect: COR003
g = df.filter(F.col("a") == 1 & F.col("b").isNull())  # expect: COR003
h = F.when(F.col("a") > 1 & F.col("b") < 2, 1).otherwise(0)  # expect: COR003
cond = F.col("a") > 1
i = df.filter(cond & F.col("b") < 2)  # expect: COR003
# A single comparison with a Column inside the `&`: computes `1 & F.col("b")`.
j = df.filter(F.col("a") > 1 & F.col("b"))  # expect: COR003
