from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
e = df.rdd.isEmpty()  # expect: DBX002
ids = df.rdd.map(lambda r: r[0]).collect()  # expect: DBX002
