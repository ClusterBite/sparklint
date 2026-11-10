import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Enabling, or other codegen / reader settings.
spark.conf.set("spark.sql.codegen.wholeStage", "true")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", True)
spark.conf.set("spark.sql.codegen.maxFields", "200")
spark.conf.set("spark.sql.codegen.fallback", "false")
spark.conf.set("spark.sql.parquet.columnarReaderBatchSize", "4096")
spark.conf.set("spark.sql.inMemoryColumnarStorage.enableVectorizedReader", "false")

# Reading the setting.
enabled = spark.conf.get("spark.sql.codegen.wholeStage")

# Value not known statically.
spark.conf.set("spark.sql.codegen.wholeStage", os.environ["CODEGEN"])

# Look-alikes.
settings = {"spark.sql.codegen.wholeStage": "false"}
spark.sql("SELECT 'spark.sql.codegen.wholeStage = false' AS note")
spark.read.option("spark.sql.parquet.enableVectorizedReader", "false").parquet("/p")


class Cfg:
    def set(self, k, v):
        pass


Cfg().set("spark.sql.codegen.wholeStage", "false")

spark.conf.set("spark.sql.codegen.wholeStage", "false")  # noqa: CFG014
