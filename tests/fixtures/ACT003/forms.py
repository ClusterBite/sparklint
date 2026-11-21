from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.pandas as ps
import pyarrow as pa

spark = SparkSession.builder.getOrCreate()
df = spark.table("events")

table = df.toArrow()  # expect: ACT003
picked = df.select("a", "b").toArrow()  # expect: ACT003
kept = df.filter(F.col("x") > 1).toArrow()  # expect: ACT003
series = ps.read_table("events")["x"].to_numpy()  # expect: ACT003
history = spark.sql("DESCRIBE HISTORY events").toPandas()  # expect: ACT003

# Not reported: bounded or driver-sized frames, and non-Spark objects.
few = df.limit(100).toArrow()
one = df.agg(F.max("x")).toArrow()
stats = df.describe().toPandas()
detail = spark.sql("DESCRIBE DETAIL events").toPandas()
head = ps.read_table("events").head(10).to_pandas()
local = pa.table({"a": [1, 2]}).to_pandas()
