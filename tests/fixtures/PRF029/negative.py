from pyspark import StorageLevel
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")

# Unreplicated levels and plain cache.
a = df.persist(StorageLevel.MEMORY_AND_DISK)
b = df.persist(StorageLevel.DISK_ONLY)
c = df.persist()
d = df.cache()
e = df.persist(StorageLevel(True, True, False, False, 1))
f = df.persist(StorageLevel(True, True, False, False))

# Replication that is not a literal.
n = int(spark.conf.get("my.replication", "1"))
g = df.persist(StorageLevel(True, True, False, False, n))

# A level from a parameter or a variable with several bindings.
def keep(frame, level):
    return frame.persist(level)


lvl = StorageLevel.MEMORY_ONLY
if spark.conf.get("x") == "1":
    lvl = StorageLevel.MEMORY_ONLY_2
h = df.persist(lvl)


# User classes with persist / cacheTable.
class Store:
    MEMORY_AND_DISK_2 = 2

    def persist(self, level):
        return level

    def cacheTable(self, name, level):
        return name


s = Store()
s.persist(StorageLevel.MEMORY_AND_DISK_2)
s.cacheTable("t", StorageLevel.MEMORY_AND_DISK_2)
s.persist(Store.MEMORY_AND_DISK_2)

# Unrelated objects with a _2 attribute.
import enum


class Level(enum.Enum):
    MEMORY_AND_DISK_2 = 2


i = df.persist(Level.MEMORY_AND_DISK_2)

# SQL that is not a replicated CACHE TABLE.
spark.catalog.cacheTable("events")
spark.catalog.cacheTable("events", StorageLevel.MEMORY_AND_DISK)
spark.sql("CACHE TABLE hot OPTIONS ('storageLevel' 'MEMORY_AND_DISK') SELECT * FROM events")
spark.sql("SELECT 'storageLevel' AS k, 'MEMORY_ONLY_2' AS v")
spark.sql("UNCACHE TABLE hot")

# Suppressed.
j = df.persist(StorageLevel.MEMORY_AND_DISK_2)  # noqa: PRF029
