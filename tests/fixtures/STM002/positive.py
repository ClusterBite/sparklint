from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

src = spark.readStream.table("raw")
q = src.writeStream.trigger(once=True).option("checkpointLocation", "/c").toTable("t")  # expect: STM002
