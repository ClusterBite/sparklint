from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

a = spark.read.csv("/x.csv", header=True)  # expect: IOD001
b = spark.read.option("header", True).option("inferSchema", True).csv("/x")  # expect: IOD001
c = spark.read.json("/x.json")  # expect: IOD001
d = spark.read.format("csv").load("/x")  # expect: IOD001
e = spark.read.load("/x", "json")  # expect: IOD001
f = spark.read.options(**{"header": True}).csv("/x")  # expect: IOD001
