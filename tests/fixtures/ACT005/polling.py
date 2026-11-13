import time
from time import sleep

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


def wait_for_table(table_name, timeout_duration=120):
    # Polls until another job has filled the table: sleeping between checks.
    i = 0
    while not spark.catalog.tableExists(table_name) or spark.table(table_name).count() == 0:
        time.sleep(1)
        if i > timeout_duration:
            raise Exception(f"table {table_name} is still empty")
        i += 1


def wait_for_files(path, limit):
    rows = 0
    started = time.time()
    while time.time() - started < limit and rows == 0:
        sleep(1)
        try:
            rows = spark.read.format("csv").load(path).count()
        except Exception:
            pass


def wait_for_rescued(folder, run_stream):
    # The condition re-reads the same source each time; only the stream started in
    # the body (outside this frame's lineage) can change the answer.
    i = 0
    while spark.read.load(folder).count() == 0:
        run_stream(folder)
        i += 1


def keep_polling(table):
    while True:
        if spark.table(table).filter(F.col("done")).count() > 0:
            break
        time.sleep(30)
