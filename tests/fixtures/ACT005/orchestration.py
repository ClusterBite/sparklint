from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Different source per iteration over a small literal list: info, still reported.
for t in ["a", "b"]:
    spark.table(t).write.mode("overwrite").saveAsTable(f"copy_{t}")  # expect: ACT005

for path in get_paths():
    spark.read.parquet(path).write.saveAsTable("x")  # expect: ACT005

for name in get_tables():
    quoted = f"`{name}`"
    hist = spark.sql(f"DESCRIBE HISTORY {quoted}")
    rows = hist.select("operation").collect()  # expect: ACT005
