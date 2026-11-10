from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
df.write.insertInto("db.t")  # expect: IOD004
df.write.insertInto("db.t", overwrite=True)  # expect: IOD004
