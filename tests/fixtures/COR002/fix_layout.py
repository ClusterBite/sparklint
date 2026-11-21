from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data")
flag = True

# The fix rewrites only the keyword and adds parentheses: comments and line
# breaks stay where they are.
a = df.filter(
    F.col("a") > 1  # expect: COR002
    and F.col("b") < 2  # maximum (año)
)
b = df.filter((F.col("a") > 1) and (F.col("b") < 2) and (F.col("c") < 3))  # expect: COR002
d = F.when(F.col("a").isNull() or F.col("a") == "", None)  # expect: COR002
e = df.filter(not F.col("flag"))  # expect: COR002 ~"'not' on a Column"
# A Python value next to a Column becomes a literal under `&`: unsafe fix only.
f = df.filter(F.col("a") > 1 and flag)  # expect: COR002
g = df.where(F.col("a") > 1 or "b > 2")  # expect: COR002
