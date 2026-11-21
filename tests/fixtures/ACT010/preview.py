import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
big = spark.table("events")
local = spark.createDataFrame(pd.DataFrame({"id": [1, 2]}))
tables = spark.sql("SHOW TABLES IN sales")
n = 20

# A preview of a frame that fits the driver is a deliberate small look: show() and
# display() give the same answer.
big.limit(10).show()
big.limit(n).show()
big.limit(10).display()
local.show()
tables.show(truncate=False)
display(big.limit(10))
display(local)
display(tables)

# Frames that may not fit the driver: a full job left in the code.
big.show()  # expect: ACT010[warning]
big.display()  # expect: ACT010[warning]
big.filter(F.col("id") > 1).show(5)  # expect: ACT010[warning]
local.join(big, "id").show()  # expect: ACT010[warning]
display(big)  # expect: ACT010[warning]
display(local.join(big, "id"))  # expect: ACT010[warning]
# One row out, but the aggregate reads every row of `big`.
big.agg(F.count("*")).show()  # expect: ACT010[warning]
