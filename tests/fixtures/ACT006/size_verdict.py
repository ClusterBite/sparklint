import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

big = spark.table("events")
pdf = pd.DataFrame({"id": [1, 2, 3]})
local = spark.createDataFrame(pdf)
rows = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
tables = spark.sql("SHOW TABLES IN sales")

# Frames that fit the driver (driver-built data, catalog metadata): counting is cheap.
if local.count() > 0:
    pass
if not rows.count():
    pass
if tables.count() == 0:
    pass
n_local = local.count()
if n_local > 0:
    pass

# Not driver-sized: local data joined with a table.
if local.join(big, "id").count() > 0:  # expect: ACT006
    pass
