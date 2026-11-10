from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
a = df.withColumn("d", F.regexp_extract("s", "(\d+)", 1))  # expect: COR016
b = df.filter(F.col("s").rlike("\bword\b"))  # expect: COR016
pattern = "\w+@\w+"  # expect: COR016
c = f"prefix\d{pattern}"  # expect: COR016
