from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# module scope of a job file: the session ends with the job
top = spark.table("t").cache()
top.count()


def ok(spark):
    df = spark.table("t").cache()
    df.count()
    df.collect()
    df.unpersist()


def ok_alias(spark):
    src = spark.table("t")
    c = src.cache()
    c.count()
    c.collect()
    src.unpersist()


def escapes(spark):
    df = spark.table("t").cache()
    df.count()
    return df


def stored(self, spark):
    df = spark.table("t").cache()
    self.df = df


def cleared(spark):
    df = spark.table("t").cache()
    df.count()
    spark.catalog.clearCache()


def derived_escapes(spark):
    base = spark.table("t").cache()
    base.count()
    wc = base.alias("wc")
    out = wc.join(spark.table("u"), "k")
    return out
