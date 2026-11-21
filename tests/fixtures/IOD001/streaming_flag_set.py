from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# The session flag set in the file: the query starts, the schema is still fixed.
spark.conf.set('spark.sql.streaming.schemaInference', 'true')
spark.readStream.json('/in')  # expect: IOD001[warning] ~"infers its schema" help~"readStream"
