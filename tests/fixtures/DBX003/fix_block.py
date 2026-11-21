from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.json("/landing")
a = df.withColumn("off", F.input_file_block_start())  # expect: DBX003
b = df.withColumn("len", F.input_file_block_length())  # expect: DBX003
