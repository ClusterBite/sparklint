from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

a = spark.table("a")
b = spark.table("b")
j = a.join(b, "id")  # expect: STY002
j2 = a.join(b, on=["id", "day"])  # expect: STY002
