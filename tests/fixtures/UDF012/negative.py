from functools import lru_cache

import pandas as pd
import psycopg2
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")


# foreachPartition / mapPartitions: one connection per partition.
def save_partition(rows):
    conn = psycopg2.connect("dbname=x")
    for row in rows:
        conn.cursor().execute("insert ...", (row.a,))


df.foreachPartition(save_partition)
df.rdd.foreachPartition(save_partition)


def enrich_partition(rows):
    with requests.Session() as s:
        for r in rows:
            yield s.get(f"https://api/{r.k}").text


df.rdd.mapPartitions(enrich_partition).count()


# ForeachWriter.open creates the connection once; process reuses it.
class Writer:
    def open(self, partition_id, epoch_id):
        self.conn = psycopg2.connect("dbname=x")
        return True

    def process(self, row):
        self.conn.cursor().execute("insert ...", (row.a,))

    def close(self, error):
        self.conn.close()


df.writeStream.foreach(Writer()).start()

# Pooled module-level client reused per row.
SESSION = requests.Session()


@F.udf("string")
def enrich(x):
    return SESSION.get(f"https://api/{x}").text


@lru_cache(maxsize=1)
def client():
    return requests.Session()


@F.udf("string")
def enrich_cached(x):
    return client().get(f"https://api/{x}").text


# pandas UDFs are batch-level (UDF011's territory, not per row).
@pandas_udf("string")
def batch(s: pd.Series) -> pd.Series:
    return s.map(lambda x: x)


# Driver-side requests (not inside a row function).
config = requests.get("https://config").json()
df2 = df.withColumn("v", F.lit(config["v"]))

# foreachBatch runs per micro-batch on the driver.
def sink(batch_df, batch_id):
    requests.post("https://hook", json={"id": batch_id})


df.writeStream.foreachBatch(sink).start()

# pandas / Python look-alikes: not Spark receivers.
pdf = pd.DataFrame({"a": [1]})
pdf["a"].map(lambda x: requests.get(str(x)))
list(map(lambda x: requests.get(str(x)), [1, 2]))


class Rows:
    def foreach(self, f):
        return f


Rows().foreach(lambda r: requests.get(r))


# A user function called `get` is not requests.get.
def get(x):
    return x


@F.udf("string")
def local_get(x):
    return get(x)
