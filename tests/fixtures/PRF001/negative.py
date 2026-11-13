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


# Another library's builder that mirrors withColumn, reached through a tuple
# returned by a helper: withColumn(name, "type", **opts) is no DataFrame call
# (DataFrame.withColumn needs a Column and takes no other keywords).
def make_gen(spark, n):
    import dbldatagen as dg

    return [f"c{i}" for i in range(n)], dg.DataGenerator(spark, rows=10)


def string_frame(spark, opts):
    names, gen = make_gen(spark, 4)
    for name in names:
        gen = gen.withColumn(name, "string", **opts)
    for name in names:
        gen = gen.withColumn(name, "int", minValue=0, maxValue=9)
    return gen.build()
