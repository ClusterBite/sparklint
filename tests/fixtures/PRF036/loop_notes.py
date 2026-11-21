import time
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.getOrCreate()
df = spark.table('t')
b = spark.table('b')
# Loop notes: only a loop that repeats the statement gets one (not a retry
# loop that breaks on success, not a first-match loop that returns).
x = df.join(b.hint('shuffle_replicate_nl'), 'k')  # expect: PRF036 !~"Inside a loop"
for t in ['a', 'b']:
    x = df.join(b.hint('shuffle_replicate_nl'), 'k')  # expect: PRF036 ~"Inside a loop"
for i in range(3):
    try:
        x = df.join(b.hint('shuffle_replicate_nl'), 'k')  # expect: PRF036 !~"Inside a loop"
        break
    except Exception:
        time.sleep(5)
def first(ts):
    for t in ts:
        if t:
            x = df.join(b.hint('shuffle_replicate_nl'), 'k')  # expect: PRF036 !~"Inside a loop"
            return x
for p in ['a', 'b']:
    for i in range(3):
        try:
            x = df.join(b.hint('shuffle_replicate_nl'), 'k')  # expect: PRF036 ~"Inside a loop"
            break
        except Exception:
            time.sleep(5)
