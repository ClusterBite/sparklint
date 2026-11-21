from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()
a = spark.table('a')
b = spark.table('b')
near = F.udf(lambda x, y: abs(x - y) < 5, 'boolean')
# A small (broadcast / hinted / limited) side lowers the finding to info.
a.join(b, near(a.x, b.x))  # expect: UDF014[warning]
a.join(F.broadcast(b), near(a.x, b.x))  # expect: UDF014[info]
F.broadcast(a).join(b, near(a.x, b.x))  # expect: UDF014[info]
a.join(b.hint('broadcast'), near(a.x, b.x))  # expect: UDF014[info]
a.join(b.limit(5), near(a.x, b.x))  # expect: UDF014[info]
a.crossJoin(b).where(near(a.x, b.x))  # expect: UDF014[warning]
a.hint('mapjoin').crossJoin(b).where(near(a.x, b.x))  # expect: UDF014[info]
a.crossJoin(b.hint('broadcast')).where(near(a.x, b.x))  # expect: UDF014[info]
