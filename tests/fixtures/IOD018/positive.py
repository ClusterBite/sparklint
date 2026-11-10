from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Daily job: write one new partition, then rediscover all of them.
spark.read.table("bronze.events").write.mode("append").partitionBy("dt").parquet("/mnt/lake/events")
spark.sql("MSCK REPAIR TABLE lake.events")  # expect: IOD018

# lower case, trailing semicolon, backticked names
spark.sql("msck repair table `lake`.`clicks`;")  # expect: IOD018

# Spark 3.2+ form with an explicit mode
spark.sql("REPAIR TABLE lake.orders SYNC PARTITIONS")  # expect: IOD018

# Hive-style ALTER form
spark.sql("ALTER TABLE lake.sales RECOVER PARTITIONS")  # expect: IOD018

# f-string table name, keyword argument
table = "lake.views"
spark.sql(sqlQuery=f"MSCK REPAIR TABLE {table}")  # expect: IOD018

# query bound to a name once
REPAIR = "REPAIR TABLE lake.sessions"
spark.sql(REPAIR)  # expect: IOD018

# leading comment
spark.sql(  # expect: IOD018
    """
    -- make the new partitions visible
    MSCK REPAIR TABLE lake.audit
    """
)

# Catalog API equivalent
spark.catalog.recoverPartitions("lake.events")  # expect: IOD018


def refresh(session: SparkSession, name: str) -> None:
    # typed-parameter session
    session.sql(f"MSCK REPAIR TABLE {name}")  # expect: IOD018


# inside a loop (warning): one full listing per table
for t in ["lake.a", "lake.b", "lake.c"]:
    spark.sql(f"MSCK REPAIR TABLE {t}")  # expect: IOD018
    spark.catalog.recoverPartitions(t)  # expect: IOD018
