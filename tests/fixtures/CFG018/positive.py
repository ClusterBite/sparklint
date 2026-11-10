from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# The SQL key is a session setting.
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "false")  # expect: CFG018
spark.conf.set("SPARK.SQL.INMEMORYCOLUMNARSTORAGE.COMPRESSED", False)  # expect: CFG018
spark.sql("SET spark.sql.inMemoryColumnarStorage.compressed = false")  # expect: CFG018

KEY = "spark.sql.inMemoryColumnarStorage.compressed"
spark.conf.set(KEY, "false")  # expect: CFG018

# The core keys take effect at launch: builder and SparkConf.
session = (
    SparkSession.builder.appName("job")
    .config("spark.shuffle.compress", "false")  # expect: CFG018
    .config("spark.shuffle.spill.compress", False)  # expect: CFG018
    .getOrCreate()
)
session2 = SparkSession.builder.config(map={"spark.broadcast.compress": "FALSE"}).getOrCreate()  # expect: CFG018
conf = SparkConf().set("spark.shuffle.spill.compress", "false")  # expect: CFG018
conf2 = SparkConf().setAll([("spark.shuffle.compress", "false")])  # expect: CFG018
CORE = "spark.broadcast.compress"
conf3 = SparkConf().setIfMissing(CORE, "false")  # expect: CFG018


def cache_raw(spark: SparkSession, df):
    spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "false")  # expect: CFG018
    df.cache().count()
    spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
