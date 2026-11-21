import time

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

url = "jdbc:postgresql://db/x"
df = spark.table("events")

# Partition counts held in a name bound once to a constant are read.
MANY = 400
df.coalesce(MANY).write.jdbc(url, "events")  # expect: IOD017
df.repartition(MANY // 2).write.jdbc(url, "events")  # expect: IOD017
FEW = 8
df.coalesce(FEW).write.jdbc(url, "events")
df.repartition(numPartitions=FEW).write.jdbc(url, "events")
df.repartition(8, "k").write.jdbc(url, "events")

# A key column held in a constant repartitions by key: the count stays high.
KEY = "k"
df.repartition(KEY).write.jdbc(url, "events")  # expect: IOD017

# numPartitions set in any spelling: capped.
df.write.jdbc(url, "events", properties={"numPartitions": "8"})
df.write.format("jdbc").option("NUMPARTITIONS", 8).option("url", url).save()

# A count of unknown value (a parameter) is trusted.


def export(n: int) -> None:
    df.coalesce(n).write.jdbc(url, "events")


# Retry loop: the write runs once (still reported, at the plain level).
for attempt in range(3):
    try:
        df.write.jdbc(url, "events", mode="append")  # expect: IOD017
        break
    except Exception:
        time.sleep(5)
