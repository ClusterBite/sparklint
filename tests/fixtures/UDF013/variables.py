from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.table("events")


@F.udf("string")
def row_key(r):
    return str(r)


# The whole-row struct held in a variable bound once.
row = F.struct("*")
a = df.withColumn("k", row_key(row))  # expect: UDF013

every = F.struct(*df.columns)
b = df.select(row_key(every))  # expect: UDF013

# Passed as a keyword argument.
c = df.withColumn("k", row_key(r=F.struct("*")))  # expect: UDF013

# Traps: a chosen subset in a variable, a name bound on two branches, a built-in.
some = F.struct("id", "name")
d = df.withColumn("k", row_key(some))

if spark.conf.get("x") == "1":
    either = F.struct("*")
else:
    either = F.struct("id")
e = df.withColumn("k", row_key(either))

j = F.to_json(F.struct("*"))
f = df.withColumn("k", row_key(j))
