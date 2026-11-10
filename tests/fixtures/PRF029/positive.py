import pyspark
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.storagelevel import StorageLevel as SL

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")

a = df.persist(StorageLevel.MEMORY_AND_DISK_2)  # expect: PRF029
b = df.persist(storageLevel=StorageLevel.MEMORY_ONLY_2)  # expect: PRF029
c = df.persist(SL.DISK_ONLY_3)  # expect: PRF029
d = df.persist(pyspark.StorageLevel.MEMORY_AND_DISK_DESER_2)  # expect: PRF029

LEVEL = StorageLevel.MEMORY_AND_DISK_2
e = df.persist(LEVEL)  # expect: PRF029

custom = StorageLevel(True, True, False, False, 2)
f = df.persist(custom)  # expect: PRF029
g = df.persist(StorageLevel(True, True, False, True, replication=3))  # expect: PRF029

rdd = spark.sparkContext.parallelize(range(10))
h = rdd.persist(StorageLevel.MEMORY_ONLY_2)  # expect: PRF029

spark.catalog.cacheTable("events", StorageLevel.MEMORY_AND_DISK_2)  # expect: PRF029
spark.sql("CACHE TABLE hot OPTIONS ('storageLevel' 'MEMORY_AND_DISK_2') SELECT * FROM events")  # expect: PRF029
spark.sql("cache lazy table t2 options ('storagelevel' = 'disk_only_2')")  # expect: PRF029


def prep(frame: DataFrame) -> DataFrame:
    return frame.persist(StorageLevel.MEMORY_AND_DISK_2)  # expect: PRF029


for day in ["a", "b"]:
    x = spark.read.table(day).persist(StorageLevel.MEMORY_AND_DISK_2)  # expect: PRF029
    x.count()
