from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

a = spark.table("a")
b = spark.table("b")
c = a.crossJoin(b)  # expect: PRF003
d = a.join(b)  # expect: PRF003
