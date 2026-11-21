from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

spark = SparkSession.builder.getOrCreate()
spark.udf.register("valid_email", lambda s: "@" in s, "boolean")


@udf("boolean")
def ok(s):
    return s.isascii()


df = spark.table("t")

# The predicate built earlier in a Column variable.
cond = ok(F.col("x"))
a = df.filter(cond)  # expect: UDF002
both = cond & (F.col("y") > 1)
b = df.where(both)  # expect: UDF002
via_expr = F.expr("valid_email(email)")
c = df.filter(via_expr)  # expect: UDF002

# Native predicates in a variable, and a name bound on two branches (not followed).
native = F.col("x").isNotNull()
d = df.filter(native)
if df.isStreaming:
    pick = ok(F.col("x"))
else:
    pick = F.col("x") > 1
e = df.filter(pick)
