import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

# A limit kept or sized moderately.
s1 = SparkSession.builder.config("spark.driver.maxResultSize", "1g").getOrCreate()
s2 = SparkSession.builder.config("spark.driver.maxResultSize", "4g").getOrCreate()
s3 = SparkSession.builder.config("spark.driver.maxResultSize", "15g").getOrCreate()
conf = SparkConf().set("spark.driver.maxResultSize", "2147483648")

# Other driver settings.
s4 = SparkSession.builder.config("spark.driver.memory", "0").getOrCreate()
s5 = SparkSession.builder.config("spark.driver.maxResultSizeBytes", "0").getOrCreate()
s6 = SparkSession.builder.config("spark.kryoserializer.buffer.max", "0").getOrCreate()

spark = SparkSession.builder.getOrCreate()

# A launch-time key on a running session has no effect (the static-conf rule's case).
spark.conf.set("spark.driver.maxResultSize", "0")
spark.sql("SET spark.driver.maxResultSize = 0")

# Reading the setting, or values not known statically / not a size.
limit = spark.conf.get("spark.driver.maxResultSize")
s7 = SparkSession.builder.config("spark.driver.maxResultSize", os.environ["MRS"]).getOrCreate()
s8 = SparkSession.builder.config("spark.driver.maxResultSize", "1.5g").getOrCreate()
s9 = SparkSession.builder.config("spark.driver.maxResultSize", "unlimited").getOrCreate()

# Look-alikes.
settings = {"spark.driver.maxResultSize": "0"}
spark.sql("SELECT 'spark.driver.maxResultSize = 0' AS note")
spark.read.option("spark.driver.maxResultSize", "0").parquet("/p")


class Cfg:
    def set(self, k, v):
        pass

    def config(self, k, v):
        return self


Cfg().set("spark.driver.maxResultSize", "0")
Cfg().config("spark.driver.maxResultSize", "0")

s10 = SparkSession.builder.config("spark.driver.maxResultSize", "0").getOrCreate()  # noqa: CFG017
