from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.table("t")
# The DataFrame form inside a loop.
for t in ["a", "b"]:
    c = df.cube("a", "b", "c", "d", "e").count()  # expect: PRF024 ~"CUBE over 5 columns" ~"Inside a loop"
