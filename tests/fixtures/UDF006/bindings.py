from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

spark = SparkSession.builder.getOrCreate()

# Bound on two branches, both return numbers.
if spark.conf.get("mode") == "a":
    scale = lambda x: x * 2
else:

    def scale(x):
        return x * 3


scale_udf = udf(scale)  # expect: UDF006

# register(name, f=...) without a return type.
spark.udf.register("twice", f=lambda x: x * 2)  # expect: UDF006

# Typed, or one branch the oracle cannot see: not reported.
spark.udf.register("twice_typed", f=lambda x: x * 2, returnType="long")

if spark.conf.get("mode") == "a":
    either = lambda x: x * 2
else:
    either = make_scaler()
either_udf = udf(either)
