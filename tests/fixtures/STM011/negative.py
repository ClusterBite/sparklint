import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# RocksDB providers (Apache and Databricks class names).
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
)
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.databricks.sql.streaming.state.RocksDBStateStoreProvider",
)
# A user class whose name only contains the heap provider's name.
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "com.example.HDFSBackedStateStoreProviderWrapper",
)

# Other state-store settings.
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.minDeltasForSnapshot", "10")

# Reading, unsetting, or a value not known statically.
p = spark.conf.get("spark.sql.streaming.stateStore.providerClass")
spark.conf.unset("spark.sql.streaming.stateStore.providerClass")
spark.conf.set("spark.sql.streaming.stateStore.providerClass", os.environ["PROVIDER"])

# Look-alikes.
settings = {
    "spark.sql.streaming.stateStore.providerClass": "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"
}
spark.sql("SELECT 'HDFSBackedStateStoreProvider' AS note")
spark.read.option("providerClass", "HDFSBackedStateStoreProvider").parquet("/p")


class Cfg:
    def set(self, k, v):
        pass


Cfg().set(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
)

spark.conf.set(  # noqa: STM011
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
)
