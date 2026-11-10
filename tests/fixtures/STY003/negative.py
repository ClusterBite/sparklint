from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
a = df.drop("tmp").select("*")
b = df.drop("tmp").select(*df.columns)
c = df.select("a").drop("a")
cols = ["a", "b"]
d = df.drop("tmp").select(cols)
