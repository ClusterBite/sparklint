import time
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.table('t')
df.write.jdbc('jdbc:x', 't')  # expect: IOD017[info] !~"Inside a loop"
for t in ['a', 'b']:
    df.write.jdbc('jdbc:x', 't')  # expect: IOD017[warning] ~"Inside a loop"
for i in range(3):
    try:
        df.write.jdbc('jdbc:x', 't')  # expect: IOD017[info] !~"Inside a loop"
        break
    except Exception:
        time.sleep(5)
