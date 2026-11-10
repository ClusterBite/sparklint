from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
rows = df.collect()  # expect: ACT002

sql_rows = spark.sql("SELECT * FROM table").collect()  # expect: ACT002

# reduced (filtered) DataFrames are still unbounded (reported as info)
filtered = df.where(df.status == "active")
rows2 = filtered.collect()  # expect: ACT002
sub = spark.sql("SELECT * FROM (SELECT * FROM t LIMIT 5) s JOIN u").collect()  # expect: ACT002
