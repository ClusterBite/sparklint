from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# The message names the codec the extension selects.
spark.read.csv('/x.csv.gz')  # expect: IOD015 ~"Reading gzip-compressed csv"
spark.read.csv('/x.csv.GZIP')  # expect: IOD015 ~"Reading gzip-compressed csv"
spark.read.csv('/x.csv.zst')  # expect: IOD015 ~"Reading zstd-compressed csv"
spark.read.csv('/x.csv.lz4')  # expect: IOD015 ~"Reading lz4-compressed csv"
spark.read.csv('/x.csv.snappy')  # expect: IOD015 ~"Reading snappy-compressed csv"
spark.read.csv('/x.csv.deflate')  # expect: IOD015 ~"Reading deflate-compressed csv"
