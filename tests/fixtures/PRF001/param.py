from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def trim_all(frame: DataFrame, cols: list) -> DataFrame:
    for c in cols:
        frame = frame.withColumn(c, F.trim(c))  # expect: PRF001
    return frame
