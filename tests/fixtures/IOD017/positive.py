from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

url = "jdbc:postgresql://db/x"
df = spark.table("events")

df.write.jdbc(url, "events", mode="append")  # expect: IOD017
df.write.jdbc(url, "events", "append", {"user": "u", "batchsize": "10000"})  # expect: IOD017
props = {"user": "u"}
df.write.jdbc(url=url, table="events", properties=props)  # expect: IOD017
(
    df.write.format("jdbc")
    .option("url", url)
    .option("dbtable", "events")
    .mode("append")
    .save()  # expect: IOD017
)
df.write.format("jdbc").options(url=url, dbtable="events").save()  # expect: IOD017
opts = {"url": url, "dbtable": "events"}
df.write.format("jdbc").options(**opts).save()  # expect: IOD017
df.write.save(format="jdbc", url=url, dbtable="events")  # expect: IOD017
w = df.write.format("jdbc").option("url", url)
w.save()  # expect: IOD017

# shuffled after the frame was coalesced: the partition count is back to 200
df.coalesce(4).groupBy("k").count().write.jdbc(url, "agg")  # expect: IOD017
# repartition by key / a large count does not cap the connections
df.repartition("k").write.jdbc(url, "events")  # expect: IOD017
df.repartition(400).write.jdbc(url, "events")  # expect: IOD017
df.repartition(F.col("k")).write.jdbc(url, "events")  # expect: IOD017


def export(frame: DataFrame, table: str) -> None:
    frame.filter("x > 0").write.mode("overwrite").jdbc(url, table)  # expect: IOD017


for t in ["a", "b"]:
    spark.table(t).write.jdbc(url, t)  # expect: IOD017
