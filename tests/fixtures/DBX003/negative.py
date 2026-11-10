from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.json("/landing")
a = df.withColumn("src", F.col("_metadata.file_path"))


def input_file_name():
    return "x"


b = input_file_name()
