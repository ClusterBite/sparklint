from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.read.csv('/x', inferSchema=True)  # expect: IOD001[warning] help~"Pass an explicit schema"
spark.read.csv('/x', header=True)  # expect: IOD001[info] help~"Pass an explicit schema"
spark.read.json('/x')  # expect: IOD001[warning] help~"Pass an explicit schema"
spark.read.parquet('/x', mergeSchema=True)  # expect: IOD001[warning] help~"Drop mergeSchema"
spark.readStream.json('/in')  # expect: IOD001[error] help~"Pass an explicit schema"
spark.readStream.format('avro').load('/in')  # expect: IOD001[error] help~"Pass an explicit schema"
