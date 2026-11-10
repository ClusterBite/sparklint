from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.getOrCreate()

url = "jdbc:postgresql://db/x"
df = spark.table("events")

# numPartitions set: Spark coalesces to it
df.write.jdbc(url, "events", properties={"numPartitions": "8"})
df.write.format("jdbc").option("url", url).option("numPartitions", 8).save()
df.write.format("jdbc").options(url=url, numPartitions=8).save()
df.write.format("jdbc").option("NumPartitions", "8").save()

# explicitly capped partition count
df.coalesce(8).write.jdbc(url, "events")
df.repartition(16).select("a").write.jdbc(url, "events")
small = df.coalesce(1)
small.write.format("jdbc").option("url", url).save()


def export(frame: DataFrame, n: int) -> None:
    frame.repartition(n).write.jdbc(url, "events")


# options we cannot see may hold numPartitions
def write_with(frame: DataFrame, opts: dict, props) -> None:
    frame.write.format("jdbc").options(**opts).save()
    frame.write.jdbc(url, "events", properties=props)


w = df.write.format("jdbc")
w.option("numPartitions", 4)
w.save()

# other formats, reads
df.write.format("delta").save("/p")
df.write.parquet("/p")
r = spark.read.jdbc(url, "events")


class Writer:
    def jdbc(self, url, table):
        pass

    def save(self):
        pass


Writer().jdbc(url, "t")
df.write.jdbc(url, "events")  # noqa: IOD017
