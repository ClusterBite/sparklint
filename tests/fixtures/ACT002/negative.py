from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
small = df.limit(100).collect()
total = df.agg(F.sum("x")).collect()
my_list = [1, 2, 3]
result = my_list.collect()
items = get_items()
other = items.collect()
probe = spark.sql("SELECT 1 FROM t LIMIT 1").collect()
table = "x"
probe2 = spark.sql(f"SELECT * FROM {table} limit 10;").collect()
