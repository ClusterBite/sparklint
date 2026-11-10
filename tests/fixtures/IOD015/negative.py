import gzip

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Splittable or columnar sources.
a = spark.read.csv("/raw/orders.csv")
b = spark.read.csv("/raw/orders.csv.bz2")
c = spark.read.parquet("/raw/orders.parquet.gz")
d = spark.read.format("parquet").load("/raw/x.gz")
e = spark.read.format("delta").load("/delta/orders")
p = input()
f = spark.read.csv(p)
g = spark.read.csv(f"/raw/{p}")
h = spark.read.format("cloudFiles").option("cloudFiles.format", "parquet").load("/x/*.gz")

# pandas / stdlib look-alikes.
pdf = pd.read_csv("/raw/orders.csv.gz")
pj = pd.read_json("/raw/e.json.gz", lines=True)
with gzip.open("/raw/orders.csv.gz") as fh:
    fh.read()


# A user class with csv/json methods.
class Loader:
    def csv(self, path):
        return path


l1 = Loader().csv("/raw/orders.csv.gz")


# Untyped parameter: not provably a Spark reader.
def untyped(reader):
    return reader.csv("/raw/orders.csv.gz")


# RDD textFile is not a DataFrameReader read.
r = spark.sparkContext.textFile("/logs/app.log.gz")
