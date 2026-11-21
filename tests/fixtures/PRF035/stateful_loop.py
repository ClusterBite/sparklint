from pyspark.sql import SparkSession
from pyspark.sql.streaming.state import GroupStateTimeout

spark = SparkSession.builder.getOrCreate()


def score(key, pdfs, state):
    return pdfs


# The stateful form inside a loop.
for topic in ["a", "b"]:
    stream = spark.readStream.table(topic)
    s = stream.groupBy().applyInPandasWithState(  # expect: PRF035 ~"the whole stream is one group" ~"Inside a loop"
        score, "a long", "a long", "append", GroupStateTimeout.NoTimeout
    )
