from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


def write_once(events: DataFrame, days):
    frames = [events.filter(F.col("day") == d) for d in days]
    reduce(DataFrame.unionByName, frames).write.mode("append").saveAsTable("cat.db.daily")


def one_table_per_iteration(tables):
    # Orchestration: every iteration writes a different table.
    for t in tables:
        spark.table(f"raw.{t}").write.mode("append").saveAsTable(f"bronze.{t}")


def path_per_iteration(df: DataFrame, days):
    for d in days:
        df.filter(F.col("day") == d).write.mode("append").parquet(f"/mnt/out/day={d}")


def target_bound_in_loop(df: DataFrame, specs):
    for spec in specs:
        name = spec["table"]
        df.write.mode("append").saveAsTable(name)


def chunked_replace(df: DataFrame, chunks):
    # Idempotent chunked backfill: each iteration replaces its own slice (ACT005 only).
    for start, end in chunks:
        df.write.mode("overwrite").option("replaceWhere", f"d >= '{start}' AND d <= '{end}'").saveAsTable("cat.db.t")


def overwrite_in_loop(df: DataFrame, parts):
    for p in parts:
        df.filter(F.col("p") == p).write.insertInto("cat.db.t", overwrite=True)


def unknown_mode(df: DataFrame, parts, mode):
    for p in parts:
        df.filter(F.col("p") == p).write.mode(mode).saveAsTable("cat.db.t")


def not_in_a_loop(df: DataFrame):
    df.write.mode("append").saveAsTable("cat.db.t")


def nested_def_not_loop(df: DataFrame, parts):
    for p in parts:
        def sink(batch_df, batch_id):
            batch_df.write.mode("append").saveAsTable("cat.db.t")
        df.writeStream.foreachBatch(sink).start()


class Writer:
    def __init__(self):
        self.write = None

    def run(self, items):
        for i in items:
            self.write.mode("append").saveAsTable("t")


def pandas_append(pdf, parts):
    for p in parts:
        pdf.to_csv("/tmp/out.csv", mode="a")


def suppressed(df: DataFrame, parts):
    for p in parts:
        df.filter(F.col("p") == p).write.mode("append").saveAsTable("cat.db.t")  # noqa: IOD006
