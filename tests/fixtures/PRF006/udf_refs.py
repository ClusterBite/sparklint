from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


def score(x):
    return x * 2.0


spark.udf.register("score", score, "double")
scale = F.udf(score, "double")

# A UDF registered with spark.udf.register and called from SQL text is expensive
# lineage, as a UDF value is.
by_expr = spark.table("events").withColumn("s", F.expr("score(v)"))  # expect: PRF006
by_expr.write.saveAsTable("a")
by_expr.write.saveAsTable("b")

by_select_expr = spark.table("events").selectExpr("id", "score(v) AS s")  # expect: PRF006
by_select_expr.write.saveAsTable("c")
print(by_select_expr.count())

# A UDF behind a Column variable.
scaled = scale(F.col("v"))
by_column = spark.table("events").withColumn("s", scaled)  # expect: PRF006
by_column.write.saveAsTable("d")
print(by_column.count())

# Built-in SQL functions are not UDFs.
cheap = spark.table("events").withColumn("s", F.expr("upper(v)"))
cheap.write.saveAsTable("e")
print(cheap.count())
