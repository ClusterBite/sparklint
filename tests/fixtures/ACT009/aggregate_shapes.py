from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("events")

# A list of global aggregates is one row.
checks = df.select([F.count("a").alias("n_a"), F.count("b").alias("n_b")]).collect()[0]

# Aggregate SQL expressions are one row.
bounds = df.selectExpr("min(v) as lo", "max(v) as hi").collect()[0]
var = df.selectExpr("variance(label - prediction)").collect()[0][0]
top_id = int(df.selectExpr("max(order_id)").collect()[0][0])
summary = df.selectExpr(["min(a) as lo", "max(a) as hi"])
row = summary.collect()[0]

# A grouping without keys is one global group.
values = df.select("code").groupBy().agg(F.min("code"), F.max("code")).collect()[0]
totals = df.groupBy().sum("amount").collect()[0]
pivoted = df.groupby().pivot("kind", ["a", "b"]).count().collect()[0]
