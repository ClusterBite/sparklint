from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# Each read gets its own name-bound reader (a reader used twice has unknown options).
r1 = spark.read.format('jdbc').option('url', 'u')
r1.option('dbtable', 'orders').load()  # expect: IOD002[warning]
r2 = spark.read.format('jdbc').option('url', 'u')
r2.option('query', 'select * from orders').load()  # expect: IOD002[warning]
r3 = spark.read.format('jdbc').option('url', 'u')
r3.option('query', 'select * from orders where id = 1').load()  # expect: IOD002[info]
r4 = spark.read.format('jdbc').option('url', 'u')
r4.option('dbtable', '(select * from t where\n x > 1) q').load()  # expect: IOD002[info]
r5 = spark.read.format('jdbc').option('url', 'u')
r5.option('dbtable', '(select * from t limit 5) q').load()  # expect: IOD002[info]
r6 = spark.read.format('jdbc').option('url', 'u')
r6.option('dbtable', '(SELECT TOP 10 * FROM t) q').load()  # expect: IOD002[info]
r7 = spark.read.format('jdbc').option('url', 'u')
r7.option('dbtable', '(select top from t) q').load()  # expect: IOD002[warning]
r8 = spark.read.format('jdbc').option('url', 'u')
r8.option('dbtable', "(select * from t where_x) q").load()  # expect: IOD002[warning]
r9 = spark.read.format('jdbc').option('url', 'u')
r9.option('dbtable', "(select * from t where n = ' limit 1 ') q").load()  # expect: IOD002[info]
r10 = spark.read.format('jdbc').option('url', 'u')
r10.option('dbtable', "(select ' limit 1 ' from t) q").load()  # expect: IOD002[warning]
r11 = spark.read.format('jdbc').option('url', 'u')
r11.option('query', 'select 1').load()  # expect: IOD002 help~"`query`"
