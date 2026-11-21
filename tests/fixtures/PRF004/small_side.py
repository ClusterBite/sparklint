from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()
a = spark.table('a')
b = spark.table('b')
near = F.udf(lambda x, y: abs(x - y) < 5, 'boolean')
# A small (broadcast / hinted / limited) side lowers the finding to info.
a.join(b, a.ts >= b.lo)  # expect: PRF004[warning]
a.join(F.broadcast(b), a.ts >= b.lo)  # expect: PRF004[info]
a.join(b.hint('broadcast'), a.ts >= b.lo)  # expect: PRF004[info]
a.join(b.hint('MAPJOIN'), a.ts >= b.lo)  # expect: PRF004[info]
a.hint('broadcastjoin').join(b, a.ts >= b.lo)  # expect: PRF004[info]
a.join(b.hint('shuffle_hash'), a.ts >= b.lo)  # expect: PRF004[warning]
a.join(b.limit(5), a.ts >= b.lo)  # expect: PRF004[info]
