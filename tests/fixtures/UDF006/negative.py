from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


@udf("long")
def double(x):
    return x * 2


@udf(returnType=IntegerType())
def inc(x):
    return x + 1


@udf
def upper(s):
    return s.upper() if s else None


@udf
def label(x) -> str:
    return f"v{x}"


typed = F.udf(lambda x: x + 1, IntegerType())
named = F.udf(lambda s: s.strip(), "string")
spark.udf.register("dbl", lambda x: x * 2.0, "double")
already = F.udf(lambda x: x, "int")
spark.udf.register("again", already)
