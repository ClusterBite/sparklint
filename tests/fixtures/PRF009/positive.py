from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


def build(spark):
    df = spark.table("t").groupBy("k").count().cache()  # expect: PRF009
    df.count()
    df.write.saveAsTable("x")


def build2(spark):
    d = spark.table("t")
    d.persist()  # expect: PRF009
    d.count()
    d.collect()
