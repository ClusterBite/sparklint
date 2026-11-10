from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
# one job, outside a loop
df.groupBy("country").count().show()
n = df.count()

items = [1, 2, 3]
for x in items:
    items.count(x)
    "a,b".count(",")


def build_jobs():
    def run():
        return df.count()  # not in a loop of its own function
    return [run for _ in range(3)]
