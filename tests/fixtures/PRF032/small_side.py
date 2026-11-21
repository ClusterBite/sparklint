from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()
a = spark.table('a')
b = spark.table('b')
near = F.udf(lambda x, y: abs(x - y) < 5, 'boolean')
# A small (broadcast / hinted / limited) side lowers the finding to info.
a.join(b, (a.k == b.k) | (a.j == b.k))  # expect: PRF032[warning]
a.join(b.hint('broadcast'), (a.k == b.k) | (a.j == b.k))  # expect: PRF032[info]
