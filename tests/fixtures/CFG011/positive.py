from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Too large / unlimited (warning).
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1000000")  # expect: CFG011
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 100000)  # expect: CFG011
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 0)  # expect: CFG011
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "-1")  # expect: CFG011
spark.conf.set("SPARK.SQL.EXECUTION.ARROW.MAXRECORDSPERBATCH", "500000")  # expect: CFG011
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(200 * 1000))  # expect: CFG011
spark.sql("SET spark.sql.execution.arrow.maxRecordsPerBatch = 250000")  # expect: CFG011

BATCH = 1_000_000
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", BATCH)  # expect: CFG011

session = (
    SparkSession.builder.appName("job")
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "200000")  # expect: CFG011
    .getOrCreate()
)
conf = SparkConf().set("spark.sql.execution.arrow.maxRecordsPerBatch", "300000")  # expect: CFG011

# Too small (info).
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "100")  # expect: CFG011


def score(spark: SparkSession, df):
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 1000000)  # expect: CFG011
    return df.toPandas()
