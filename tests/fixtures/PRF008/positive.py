from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
cached = df.cache()  # expect: PRF008
result = cached.count()

unused = df.persist()  # expect: PRF008


# Trap for the escape check: the derived frame is consumed locally once and a
# plain value (not the frame) is returned, so the cache is still single-use.
def total(src: DataFrame) -> int:
    c = src.cache()  # expect: PRF008
    n = c.filter(F.col("x") > 0).count()
    return n
