import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data")


def converge(df, i=0):
    # The condition's frame changes every iteration: one job per step, not polling.
    while df.filter(F.col("v") > i).count() > 0:  # expect: ACT005
        i += 1


def shrink(df):
    while df.count() > 1000:  # expect: ACT005
        df = df.sample(0.5)


def paced(keys):
    # A for loop that sleeps for pacing still runs one job per key.
    for k in keys:
        df.filter(F.col("k") == k).write.mode("append").saveAsTable("t")  # expect: ACT005
        time.sleep(1)


def sleep_only_in_helper(keys):
    queue = list(keys)
    while queue:
        k = queue.pop()
        print(df.filter(F.col("k") == k).count())  # expect: ACT005

        def backoff():
            time.sleep(1)

        backoff()


def outer_loop_not_polling(tables):
    for t in tables:
        n = 0
        while n < 3:
            spark.table(t).filter(F.col("p") == n).count()  # expect: ACT005
            n += 1
