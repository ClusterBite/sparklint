from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "false")  # expect: CFG015
spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", False)  # expect: CFG015
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")  # expect: CFG015
spark.conf.set("SPARK.SQL.OPTIMIZER.DYNAMICPARTITIONPRUNING.ENABLED", "FALSE")  # expect: CFG015
spark.sql("SET spark.databricks.optimizer.dynamicFilePruning=false")  # expect: CFG015

KEY = "spark.databricks.optimizer.dynamicFilePruning"
spark.conf.set(KEY, "false")  # expect: CFG015

session = (
    SparkSession.builder.appName("job")
    .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")  # expect: CFG015
    .getOrCreate()
)
session2 = SparkSession.builder.config(map={"spark.databricks.optimizer.dynamicFilePruning": "false"}).getOrCreate()  # expect: CFG015
conf = SparkConf().set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")  # expect: CFG015


def compare_plans(spark: SparkSession, q):
    spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "false")  # expect: CFG015
    spark.sql(q).explain()
    spark.conf.unset("spark.databricks.optimizer.dynamicFilePruning")
