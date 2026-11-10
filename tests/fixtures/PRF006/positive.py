from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

big = spark.table("big")
dim = spark.table("dim")
agg = big.join(dim, "k").groupBy("x").agg(F.sum("v").alias("v"))  # expect: PRF006
agg.write.mode("overwrite").saveAsTable("out")
print(agg.count())


def report(events: DataFrame):
    daily = events.groupBy("day").count()  # expect: PRF006
    for d in ["a", "b"]:
        daily.filter(F.col("day") == d).show()
        daily.filter(F.col("day") == d).collect()


raw = spark.read.json("/landing")  # expect: PRF006
good = raw.filter(F.col("ok"))
bad = raw.filter(~F.col("ok"))
good.write.saveAsTable("good")
bad.write.saveAsTable("bad")
