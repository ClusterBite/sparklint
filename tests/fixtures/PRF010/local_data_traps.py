import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

spark = SparkSession.builder.getOrCreate()

pdf = pd.DataFrame({"id": [1, 2, 3]})
big = spark.table("events")

# Not driver-side data: an RDD, or local data joined with / unioned into a table.
rdd = big.rdd.map(lambda r: (r[0], 1))
spark.createDataFrame(rdd).repartition(5).write.saveAsTable("a")  # expect: PRF010
local = spark.createDataFrame(pdf)
local.join(big, "id").repartition(5).write.saveAsTable("b")  # expect: PRF010
local.unionByName(big).repartition(5).write.saveAsTable("c")  # expect: PRF010


def load(data):
    return spark.createDataFrame(data)


load(rdd).repartition(5).write.saveAsTable("d")  # expect: PRF010

# The UDF runs before the repartition, or the step after it is a plain column.
upper = udf(lambda s: s.upper(), "string")
big.select(upper("a").alias("a")).repartition(8).write.saveAsTable("e")  # expect: PRF010
big.repartition(8).withColumn("b", F.upper("a")).write.saveAsTable("f")  # expect: PRF010
