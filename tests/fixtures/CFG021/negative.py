import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Empty lists (clearing the setting).
spark.conf.set("spark.sql.optimizer.excludedRules", "")
spark.conf.set("spark.sql.adaptive.optimizer.excludedRules", "  ")
spark.sql("SET spark.sql.optimizer.excludedRules=")

# Reading, unsetting, or a value not known statically.
rules = spark.conf.get("spark.sql.optimizer.excludedRules")
spark.conf.unset("spark.sql.optimizer.excludedRules")
spark.conf.set("spark.sql.optimizer.excludedRules", os.environ["EXCLUDED"])

# Other optimizer settings.
spark.conf.set("spark.sql.optimizer.maxIterations", "200")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.sql.optimizer.plannedWrite.enabled", "true")

# Look-alikes.
settings = {
    "spark.sql.optimizer.excludedRules": "org.apache.spark.sql.catalyst.optimizer.PushDownPredicates"
}
spark.sql("SELECT 'spark.sql.optimizer.excludedRules' AS k")
spark.read.option("excludedRules", "ColumnPruning").parquet("/p")


class Cfg:
    def set(self, k, v):
        pass


Cfg().set(
    "spark.sql.optimizer.excludedRules",
    "org.apache.spark.sql.catalyst.optimizer.PushDownPredicates",
)

spark.conf.set(  # noqa: CFG021
    "spark.sql.optimizer.excludedRules",
    "org.apache.spark.sql.catalyst.optimizer.PushDownPredicates",
)
