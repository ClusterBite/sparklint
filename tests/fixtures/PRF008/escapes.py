from contextlib import contextmanager

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


# The cached frame is yielded: the caller consumes it, any number of times.
@contextmanager
def cached_for_training(dataset: DataFrame, level: StorageLevel):
    dataset.persist(level)
    try:
        yield dataset
    finally:
        dataset.unpersist()


# A frame derived from the cache is returned: its uses are in the caller.
def prepared(df: DataFrame) -> DataFrame:
    base = df.cache()
    return base.filter(F.col("x") > 0)


# Derived frame stored on an object.
class Holder:
    def load(self, df: DataFrame) -> None:
        c = df.cache()
        self.out = c.select("a")


# Generator yielding a derived frame.
def batches(df: DataFrame):
    c = df.persist()
    yield from [c.filter(F.col("k") == 1)]


# Look-alike: a frame that is only used once locally is still reported
# (see positive.py); here a local single use plus a lambda capture is not.
def lazy(df: DataFrame):
    c = df.cache()
    return lambda: c.count()
