from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Reads over an RDD that fits the driver (the type checker's size verdict):
# inference over a few rows costs nothing.
t1 = spark.read.json(spark.createDataFrame([('{"a": 1}',)], ["j"]).rdd.map(lambda r: r.j))
t2 = spark.read.csv(sc.parallelize(c=["a,b", "1,2"]), header=True, inferSchema=True)
docs = ['{"a": 1}', '{"a": 2}']
t3 = spark.read.json(sc.parallelize(docs).filter(lambda s: s))
t4 = spark.read.json(spark.table("t").limit(10).rdd.map(lambda r: r.payload))
