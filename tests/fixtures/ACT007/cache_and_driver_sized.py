from pyspark import StorageLevel
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
orders = spark.table("orders")

# Eager-cache idiom: the count fills the cache that later actions read.
orders.cache().count()
orders.persist(StorageLevel.MEMORY_AND_DISK).count()
events = spark.table("events")
events.cache()
events.count()
cached = spark.table("items").persist()
cached.count()
orders.localCheckpoint().count()

# A frame derived from the cache is not the cache: its count is discarded work.
events.filter("kind = 'click'").count()  # expect: ACT007 ~"result is discarded"
# Collecting a cached frame and dropping the rows is still waste.
orders.cache().collect()  # expect: ACT007

# Catalog metadata and driver-built frames are cheap to count.
print(spark.sql("SHOW TABLES").count())
spark.sql("SHOW TABLES IN sales").count()
small = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
print(small.count())
tables = spark.sql("SHOW TABLES")
n_tables = tables.count()
print(n_tables)

# A count held in a name and only logged carries the help.
n = orders.count()  # expect: ACT007 help~"df.observe()"
print(f"orders: {n}")
