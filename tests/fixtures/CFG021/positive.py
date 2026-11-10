from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

PUSHDOWN = "org.apache.spark.sql.catalyst.optimizer.PushDownPredicates"

# Session setting: one rule, several rules, any spelling of the key.
spark.conf.set("spark.sql.optimizer.excludedRules", PUSHDOWN)  # expect: CFG021
spark.conf.set(  # expect: CFG021
    "spark.sql.optimizer.excludedRules",
    "org.apache.spark.sql.catalyst.optimizer.ColumnPruning,"
    "org.apache.spark.sql.catalyst.optimizer.ConstantFolding,"
    "org.apache.spark.sql.catalyst.optimizer.CollapseProject,"
    "org.apache.spark.sql.catalyst.optimizer.ReorderJoin",
)
spark.conf.set("SPARK.SQL.OPTIMIZER.EXCLUDEDRULES", "ConstantFolding")  # expect: CFG021

# The adaptive (AQE) optimizer's list.
spark.conf.set(  # expect: CFG021
    "spark.sql.adaptive.optimizer.excludedRules",
    "org.apache.spark.sql.execution.adaptive.AQEPropagateEmptyRelation",
)

# SQL SET.
spark.sql("SET spark.sql.optimizer.excludedRules = org.apache.spark.sql.catalyst.optimizer.ReorderJoin")  # expect: CFG021

# Constant key.
KEY = "spark.sql.optimizer.excludedRules"
spark.conf.set(KEY, PUSHDOWN)  # expect: CFG021

# Builder and SparkConf.
session = (
    SparkSession.builder.appName("job")
    .config("spark.sql.optimizer.excludedRules", PUSHDOWN)  # expect: CFG021
    .getOrCreate()
)
session2 = SparkSession.builder.config(map={KEY: PUSHDOWN}).getOrCreate()  # expect: CFG021
conf = SparkConf().set(KEY, PUSHDOWN)  # expect: CFG021
conf2 = SparkConf().setAll([(KEY, PUSHDOWN)])  # expect: CFG021


def tune(spark: SparkSession):
    spark.conf.set(KEY, "org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints")  # expect: CFG021
