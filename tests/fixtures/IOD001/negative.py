from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType

SCHEMA = StructType([StructField("a", StringType())])
a = spark.read.schema(SCHEMA).option("header", True).csv("/x")
b = spark.read.csv("/x", schema=SCHEMA)
c = spark.read.parquet("/x")
d = spark.read.format("delta").load("/x")
import pandas as pd
e = pd.read_csv("/x.csv")
f = pd.read_json("/x.json")

# A stored reader configured by a separate statement: the schema may be set there.
r = spark.read.format("csv")
r.schema(SCHEMA)
g = r.load("/x")
r2 = spark.read
r2.options(**{"header": True})
h = r2.csv("/x")
# Positional schema arguments.
i = spark.read.csv("/x", SCHEMA)
j = spark.read.load("/x", "json", SCHEMA)
# A `format` key in options() is an option, not the source format.
k = spark.read.options(format="csv").load("/x")
