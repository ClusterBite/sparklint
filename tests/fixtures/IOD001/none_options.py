from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
p = "/data/x"
UNSET = None

# pyspark drops `None` options: no inferSchema, so all columns are strings (info).
a = spark.read.csv(p, header=True, inferSchema=None)  # expect: IOD001[info]
b = spark.read.csv(p, inferSchema=UNSET)  # expect: IOD001[info]
# Not reported: mergeSchema=None leaves the footer merge off.
c = spark.read.parquet(p, mergeSchema=None)
