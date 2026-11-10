from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# TBLPROPERTIES text inside a string literal is not a property list.
spark.sql("COMMENT ON TABLE t IS 'TBLPROPERTIES (delta.autoOptimize.optimizeWrite = false)'")

# A comment inside the property list does not hide the key.
spark.sql("ALTER TABLE t SET TBLPROPERTIES (\n -- off for the stream\n 'delta.autoOptimize.autoCompact' = 'false')")  # expect: CFG009
