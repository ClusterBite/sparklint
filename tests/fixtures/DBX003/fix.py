from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.json("/landing")
a = df.withColumn("src", F.input_file_name())  # expect: DBX003
