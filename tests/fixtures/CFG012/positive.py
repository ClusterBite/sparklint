from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Too large (warning).
spark.conf.set("spark.sql.files.maxPartitionBytes", "2g")  # expect: CFG012
spark.conf.set("spark.sql.files.maxPartitionBytes", 4294967296)  # expect: CFG012
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "4gb")  # expect: CFG012
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "8g")  # expect: CFG012
spark.conf.set("SPARK.SQL.FILES.MAXPARTITIONBYTES", "1536m")  # expect: CFG012
spark.sql("SET spark.sql.files.maxPartitionBytes = 3g")  # expect: CFG012

BIG = "2g"
spark.conf.set("spark.sql.files.maxPartitionBytes", BIG)  # expect: CFG012

session = (
    SparkSession.builder.appName("job")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "3g")  # expect: CFG012
    .getOrCreate()
)
conf = SparkConf().set("spark.sql.files.maxPartitionBytes", "2g")  # expect: CFG012

# Too small (info).
spark.conf.set("spark.sql.files.maxPartitionBytes", "1m")  # expect: CFG012
spark.conf.set("spark.sql.files.maxPartitionBytes", 1048576)  # expect: CFG012
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "512k")  # expect: CFG012
spark.conf.set("spark.sql.files.openCostInBytes", "0")  # expect: CFG012


def load(spark: SparkSession, path):
    spark.conf.set("spark.sql.files.maxPartitionBytes", "4g")  # expect: CFG012
    df = spark.read.parquet(path)
    spark.conf.set("spark.sql.files.maxPartitionBytes", "128m")
    return df
