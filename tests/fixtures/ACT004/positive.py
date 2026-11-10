from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
out = []
for row in df.collect():  # expect: ACT004
    out.append(row.a * 2)

for r in df.toLocalIterator():  # expect: ACT004
    print(r)

doubled = [r.a * 2 for r in df.collect()]  # expect: ACT004
