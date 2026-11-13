from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
df.withColumn("x", F.lit(1))  # expect: COR007
df.dropDuplicates(["id"])  # expect: COR007
df.filter(F.col("a") > 1)  # expect: COR007
df.groupBy("a").count()  # expect: COR007
df.hint("broadcast")  # expect: COR007


def clean(frame: DataFrame):
    frame.na.fill(0)  # expect: COR007
    return frame
