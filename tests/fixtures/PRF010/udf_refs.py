from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


def score(x):
    return x * 2.0


spark.udf.register("score", score, "double")
scale = F.udf(score, "double")

# A Python UDF after the repartition spreads the UDF work over more tasks: a UDF
# registered with spark.udf.register and called from SQL text counts too.
spark.table("raw").repartition(30).withColumn("s", F.expr("score(v)")).write.saveAsTable("o1")
spark.table("raw").repartition(30).selectExpr("id", "score(v) AS s").write.saveAsTable("o2")

# ... and a UDF behind a Column variable.
scaled = scale(F.col("v"))
spark.table("raw").repartition(30).withColumn("s", scaled).write.saveAsTable("o3")

# Built-in SQL functions are not UDFs.
spark.table("raw").repartition(30).withColumn("s", F.expr("upper(v)")).write.saveAsTable("o4")  # expect: PRF010
