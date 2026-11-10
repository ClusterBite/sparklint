from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
df = df.withColumn("new_col", df.old_col + 1)
batches = [{"a": F.lit(1)}, {"c": F.lit(3)}]
for batch in batches:
    df = df.withColumns(batch)


class Builder:
    def withColumn(self, name, value):
        return self


b = Builder()
for c in ["a", "b"]:
    b = b.withColumn(c, 1)
