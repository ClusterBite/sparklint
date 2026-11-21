import time
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.table('t')
spark.sql('MSCK REPAIR TABLE t')  # expect: IOD018[info] !~"Inside a loop"
for t in ['a', 'b']:
    spark.sql('MSCK REPAIR TABLE t')  # expect: IOD018[warning] ~"Inside a loop"
for i in range(3):
    try:
        spark.sql('MSCK REPAIR TABLE t')  # expect: IOD018[info] !~"Inside a loop"
        break
    except Exception:
        time.sleep(5)
