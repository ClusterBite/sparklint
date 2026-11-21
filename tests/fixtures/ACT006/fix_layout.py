from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
# A receiver that spans lines inside its own parentheses keeps them in the fix (its
# line breaks are only valid inside brackets).
if (df  # expect: ACT006
    .filter(F.col("a") > 1)
).count() == 0:
    pass
# Line breaks inside the call's own brackets stay as written.
if df.filter(  # expect: ACT006
    F.col("a") > 1
).count() > 0:
    pass
