from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

url = "jdbc:postgresql://db/x"
props = {"user": "u"}
a = spark.read.jdbc(url, "orders", properties=props)  # expect: IOD002
b = (
    spark.read.format("jdbc")
    .option("url", url)
    .option("dbtable", "orders")
    .load()  # expect: IOD002
)
r = spark.read.format("jdbc").option("url", url)
c = r.option("dbtable", "big").load()  # expect: IOD002
opts = {"url": url, "dbtable": "orders"}
d = spark.read.format("jdbc").options(**opts).load()  # expect: IOD002
