from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

spark = SparkSession.builder.getOrCreate()

# A lambda bound to a name, then wrapped.
rev = lambda s: s[::-1]
rev_udf = F.udf(rev)  # expect: UDF001

# The function as register's keyword argument.
spark.udf.register("lower_it", f=lambda s: s.lower())  # expect: UDF001

# Bound on two branches, both the same built-in.
if spark.conf.get("mode") == "a":
    size_of = lambda xs: len(xs)
else:

    def size_of(xs):
        return len(xs)


size_udf = udf(size_of, "int")  # expect: UDF001

# Two branches with different built-ins, or one branch that is no plain function:
# no single built-in replaces the UDF.
if spark.conf.get("mode") == "a":
    case = lambda s: s.lower()
else:
    case = lambda s: s.upper()
case_udf = F.udf(case)

if spark.conf.get("mode") == "a":
    either = lambda s: s[::-1]
else:
    either = make_transform()
either_udf = F.udf(either)
