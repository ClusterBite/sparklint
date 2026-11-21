import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

big = spark.table("events")
pdf = pd.DataFrame({"id": [1, 2, 3]})
local = spark.createDataFrame(pdf)
rows = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
tables = spark.sql("SHOW TABLES IN sales")

# A cross join with a frame that fits the driver is small: info.
a = big.crossJoin(local)  # expect: PRF003[info]
b = rows.crossJoin(big)  # expect: PRF003[info]
c = big.crossJoin(big.limit(5))  # expect: PRF003[info]
d = big.crossJoin(local.join(big, "id"))  # expect: PRF003[error]
