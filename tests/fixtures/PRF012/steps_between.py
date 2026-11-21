from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.table("orders")
other = spark.table("other")

# Later column derived from the exploded value through an expression string.
a1 = df.withColumn("i", F.explode("items")).withColumn("u", F.expr("upper(i)")).filter(F.col("u") == "X")
# Later column built from a Column variable: cannot tell what it reads.
c = F.col("i") * 2
a2 = df.withColumn("i", F.explode("items")).withColumn("j", c).filter(F.col("j") > 3)
# Later column read through a DataFrame attribute of the exploded column.
x = df.withColumn("i", F.explode("items"))
a3 = x.withColumn("p", x.i.price).filter(F.col("p") > 3)
# Steps that do not commute with the filter: moving it changes the result.
b1 = df.withColumn("i", F.explode("items")).limit(10).filter(F.col("country") == "ZA")
b2 = df.withColumn("i", F.explode("items")).unionByName(other).filter(F.col("country") == "ZA")
b3 = df.withColumn("i", F.explode("items")).dropDuplicates(["id"]).filter(F.col("country") == "ZA")
b4 = df.withColumn("i", F.explode("items")).sample(0.1).filter(F.col("country") == "ZA")
# A later column whose name is not a literal.
name = "c2"
b5 = df.withColumn("i", F.explode("items")).withColumn(name, F.col("i")).filter(F.col("c2") > 1)
