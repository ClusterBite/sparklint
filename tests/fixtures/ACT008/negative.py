from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

bad = spark.table("bad_ids")
df = spark.table("events")
top = [r.id for r in bad.select("id").limit(10).collect()]
ok = df.filter(F.col("id").isin(top))
threshold = bad.agg(F.max("v")).first()[0]
ok2 = df.filter(F.col("v") > threshold)
literal = df.filter(F.col("id").isin([1, 2, 3]))
names = ["a", "b"]
ok3 = df.filter(F.col("n").isin(names))
semi = df.join(bad.select("id"), "id", "left_semi")
