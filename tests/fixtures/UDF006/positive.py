from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import udf


@udf  # expect: UDF006
def double(x):
    return x * 2


@udf()  # expect: UDF006
def is_big(x) -> bool:
    return x > 10


plus_one = F.udf(lambda x: x + 1)  # expect: UDF006


def lookup(x):
    return mapping.get(x)


mapped = udf(lookup)  # expect: UDF006
spark.udf.register("dbl", lambda x: x * 2.0)  # expect: UDF006
