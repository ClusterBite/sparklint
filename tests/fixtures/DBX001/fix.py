from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

app = spark.sparkContext.applicationId  # expect: DBX001
spark.sparkContext.setLogLevel("WARN")  # expect: DBX001
x = 1
