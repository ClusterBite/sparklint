from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("sales")

c5 = df.cube("a", "b", "c", "d", "e").count()  # expect: PRF024
c6 = df.cube(F.col("a"), "b", "c", "d", "e", "f").agg(F.sum("x"))  # expect: PRF024
c_list = df.cube(["a", "b", "c", "d", "e"]).count()  # expect: PRF024
r8 = df.rollup("a", "b", "c", "d", "e", "f", "g", "h").count()  # expect: PRF024


def report(frame: DataFrame):
    return frame.cube("y", "m", "d", "region", "store").sum("amount")  # expect: PRF024


q1 = spark.sql("SELECT a, b, count(*) FROM t GROUP BY CUBE(a, b, c, d, e)")  # expect: PRF024
q2 = spark.sql(  # expect: PRF024
    """
    SELECT a, b, c, d, e, sum(x)
    FROM t
    GROUP BY a, b, c, d, e WITH CUBE
    """
)
QUERY = "select * from t group by cube((a, b), c, d, upper(e), f)"
q3 = spark.sql(QUERY)  # expect: PRF024
table = "sales"
q4 = spark.sql(f"SELECT 1 FROM {table} GROUP BY ROLLUP(a, b, c, d, e, f, g, h)")  # expect: PRF024
