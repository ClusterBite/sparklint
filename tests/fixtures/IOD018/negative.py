from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# The recommended form: register only the partition just written.
spark.sql("ALTER TABLE lake.events ADD IF NOT EXISTS PARTITION (dt='2024-01-01')")

# Unity Catalog metadata sync for Delta / Iceberg: no partition listing.
spark.sql("REPAIR TABLE lake.events SYNC METADATA")

# Other DDL / partition statements
spark.sql("ALTER TABLE lake.events DROP IF EXISTS PARTITION (dt='2020-01-01')")
spark.sql("SHOW PARTITIONS lake.events")
spark.sql("REFRESH TABLE lake.events")
spark.catalog.refreshTable("lake.events")

# The words only inside a string literal or as an identifier
spark.sql("SELECT 'MSCK REPAIR TABLE t' AS hint")
spark.sql("SELECT * FROM ops.msck_repair_log")
spark.sql("INSERT INTO ops.log VALUES ('ALTER TABLE t RECOVER PARTITIONS')")

# Not a Spark session / catalog
class Hive:
    def sql(self, q):
        return q

    def recoverPartitions(self, t):
        return t


h = Hive()
h.sql("MSCK REPAIR TABLE lake.events")
h.recoverPartitions("lake.events")

# A plain string that is never run
doc = "Run MSCK REPAIR TABLE t after a manual copy."

# Unknown query text
def run(q):
    return spark.sql(q)


# noqa
spark.sql("MSCK REPAIR TABLE lake.events")  # noqa: IOD018
