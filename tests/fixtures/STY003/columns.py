from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
dropped = F.col("tmp")
# A star (`df["*"]`, `F.column("*")`) selects the remaining columns: not redundant.
a = df.drop("tmp").select(df["*"])
b = df.drop("tmp").select(F.column("*"), "x")
# A Column variable could be the dropped column: reported, but no fix.
c = df.drop("tmp").select(dropped)  # expect: STY003
# F.lit values and method names are not column names: fixed.
d = df.drop("x").select("a", F.lit("x").alias("k"))  # expect: STY003
e = df.drop("alias").select(F.col("a").alias("b"))  # expect: STY003
