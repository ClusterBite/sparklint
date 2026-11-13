import contextlib

import pytest
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.table("t")

# A try without an except handler, the handler/else/finally blocks, other
# context managers and statements nested deeper are still reported.
try:
    df.dropDuplicates()  # expect: COR007
finally:
    pass

try:
    df = df.filter("a > 1")
except ValueError:
    df.dropDuplicates()  # expect: COR007
else:
    df.distinct()  # expect: COR007

with contextlib.suppress(Exception):
    df.dropDuplicates()  # expect: COR007

with open("f") as fh:
    df.distinct()  # expect: COR007


def test_nested():
    with pytest.raises(ValueError):
        if True:
            df.dropDuplicates()  # expect: COR007
