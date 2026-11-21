from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# Raw file lines get the read-the-files help, other RDDs the schema help.
spark.read.json(sc.textFile('/x'))  # expect: IOD016 help~"spark.read.json(path)"
r = sc.textFile('/x')
spark.read.json(r)  # expect: IOD016 help~"spark.read.json(path)"
spark.read.json(spark.table('t').rdd.map(lambda r: r.v))  # expect: IOD016 help~"explicit schema (DDL string)"
