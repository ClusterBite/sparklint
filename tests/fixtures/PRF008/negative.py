from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
cached = df.cache()
count = cached.count()
sample = cached.limit(10).collect()
cached.write.parquet("/out")

result = some_object.cache()

looped = df.cache()
for k in ["a", "b"]:
    looped.filter(F.col("k") == k).write.parquet(f"/out/{k}")
