from pyspark import pipelines as dp
import dlt
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# Two readers: below the default threshold of 3.
@dp.table
def one():
    return spark.readStream.table("src_a")


@dp.table
def two():
    return spark.readStream.table("src_a").select("id")


# Batch reads (materialized views) are not streaming flows.
for r in ["us", "eu", "apac"]:

    @dp.materialized_view(name=f"mv_{r}")
    def mv(r=r):
        return spark.read.table("combined_orders").filter(f"region = '{r}'")


# A different source per iteration.
for r in ["us", "eu", "apac"]:

    @dp.table(name=f"per_region_{r}")
    def per_region(r=r):
        return spark.readStream.table(f"raw_{r}")


# Already fanned out with one ForEachBatch sink: a single reader.
@dp.foreach_batch_sink(name="orders_fanout")
def handler(batch_df, batch_id):
    batch_df.persist()
    for r in ["us", "eu", "apac"]:
        batch_df.filter(f"region = '{r}'").write.mode("append").saveAsTable(f"orders_{r}")
    batch_df.unpersist()


@dp.append_flow(target="orders_fanout")
def orders_once():
    return spark.readStream.table("raw_orders")


# Plain functions (not pipeline datasets) streaming the same source.
def helper1():
    return spark.readStream.table("src_b")


def helper2():
    return spark.readStream.table("src_b")


def helper3():
    return spark.readStream.table("src_b")


# One dataset reading the same source twice (self-join) counts once.
@dp.table
def self_join():
    s = spark.readStream.table("src_c")
    return s.join(spark.readStream.table("src_c"), "id")


# Look-alike: a user object with a table() method and a dlt-named variable.
class Reader:
    def table(self, name):
        return name


reader = Reader()


@dlt.table
def x1():
    return reader.table("src_d")


@dlt.table
def x2():
    return reader.table("src_d")


@dlt.table
def x3():
    return reader.table("src_d")
