from pyspark import SparkConf
from pyspark.sql import SparkSession

# Limit removed at launch: builder and SparkConf.
spark = (
    SparkSession.builder.appName("job")
    .config("spark.driver.maxResultSize", "0")  # expect: CFG017
    .getOrCreate()
)
s2 = SparkSession.builder.config("spark.driver.maxResultSize", 0).getOrCreate()  # expect: CFG017
s3 = SparkSession.builder.config("SPARK.DRIVER.MAXRESULTSIZE", "-1").getOrCreate()  # expect: CFG017
s4 = SparkSession.builder.config(map={"spark.driver.maxResultSize": "0"}).getOrCreate()  # expect: CFG017
OPTS = {"spark.driver.maxResultSize": "0", "spark.executor.memory": "8g"}
s5 = SparkSession.builder.config(map=OPTS).getOrCreate()  # expect: CFG017

conf = SparkConf().set("spark.driver.maxResultSize", "0")  # expect: CFG017
conf2 = SparkConf().setAll([("spark.driver.maxResultSize", "0")])  # expect: CFG017
KEY = "spark.driver.maxResultSize"
conf3 = SparkConf().setIfMissing(KEY, "0")  # expect: CFG017

# Raised far beyond what a driver can hold (info).
s6 = SparkSession.builder.config("spark.driver.maxResultSize", "16g").getOrCreate()  # expect: CFG017
s7 = SparkSession.builder.config("spark.driver.maxResultSize", "64GB").getOrCreate()  # expect: CFG017
conf4 = SparkConf().set("spark.driver.maxResultSize", "20480m")  # expect: CFG017
HUGE = "100g"
conf5 = SparkConf().set(KEY, HUGE)  # expect: CFG017


def build(app: str) -> SparkSession:
    return SparkSession.builder.appName(app).config("spark.driver.maxResultSize", "0").getOrCreate()  # expect: CFG017
