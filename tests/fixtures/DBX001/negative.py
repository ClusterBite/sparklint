from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

conf = spark.conf.get("spark.app.id")


class Job:
    sparkContext = None


j = Job()
x = j.sparkContext


def f(sc):
    return sc.textFile("/x")
