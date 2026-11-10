from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.sql.codegen.wholeStage", "false")  # expect: CFG014
spark.conf.set("spark.sql.codegen.wholeStage", False)  # expect: CFG014
spark.conf.set("SPARK.SQL.CODEGEN.WHOLESTAGE", "FALSE")  # expect: CFG014
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")  # expect: CFG014
spark.conf.set("spark.sql.orc.enableVectorizedReader", "false")  # expect: CFG014
spark.sql("SET spark.sql.parquet.enableVectorizedReader = false")  # expect: CFG014

KEY = "spark.sql.codegen.wholeStage"
spark.conf.set(KEY, "false")  # expect: CFG014

session = (
    SparkSession.builder.appName("job")
    .config("spark.sql.parquet.enableVectorizedReader", "false")  # expect: CFG014
    .getOrCreate()
)
session2 = SparkSession.builder.config(map={"spark.sql.codegen.wholeStage": False}).getOrCreate()  # expect: CFG014
conf = SparkConf().set("spark.sql.orc.enableVectorizedReader", "false")  # expect: CFG014


def legacy_read(spark: SparkSession, path):
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")  # expect: CFG014
    df = spark.read.parquet(path)
    df.count()
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
