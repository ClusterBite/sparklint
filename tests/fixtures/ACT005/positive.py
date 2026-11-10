from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
countries = get_countries()
for c in countries:
    n = df.filter(F.col("country") == c).count()  # expect: ACT005
    print(n)

i = 0
while df.filter(F.col("v") > i).count() > 0:  # expect: ACT005
    i += 1

totals = [df.filter(F.col("k") == k).count() for k in countries]  # expect: ACT005


def per_day(events: DataFrame, days):
    for d in days:
        events.filter(F.col("day") == d).write.mode("overwrite").option("replaceWhere", f"day = {d}").saveAsTable("t")  # expect: ACT005
