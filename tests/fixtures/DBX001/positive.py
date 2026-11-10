from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
from pyspark import SparkContext

sc2 = spark.sparkContext  # expect: DBX001
app = spark.sparkContext.applicationId  # expect: DBX001
spark.sparkContext.setLogLevel("WARN")  # expect: DBX001
ctx = SparkContext.getOrCreate()  # expect: DBX001
