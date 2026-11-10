from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # expect: CFG002
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # expect: CFG002
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 0)  # expect: CFG002
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")  # expect: CFG002
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", "-1b")  # expect: CFG002
spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")  # expect: CFG002

session = (
    SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", "-1")  # expect: CFG002
    .getOrCreate()
)
conf = SparkConf().set("spark.sql.autoBroadcastJoinThreshold", "-1")  # expect: CFG002

DISABLED = -1
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", DISABLED)  # expect: CFG002

# Scoped workaround: disabled, then unset again (reported as info).
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # expect: CFG002
spark.table("a").join(spark.table("b"), "k").write.saveAsTable("c")
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
