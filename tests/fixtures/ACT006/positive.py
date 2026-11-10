from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
if df.count() > 0:  # expect: ACT006
    pass
if df.count() == 0:  # expect: ACT006
    pass
if 0 < df.count():  # expect: ACT006
    pass
if not df.count():  # expect: ACT006
    pass
if df.count():  # expect: ACT006
    pass
empty = df.rdd.isEmpty()  # expect: ACT006
