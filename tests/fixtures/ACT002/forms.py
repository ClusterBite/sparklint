from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.table("sales")
N = 100


def helper():
    return spark.table("x").limit(5)


def keep_active(frame):
    return frame.filter("active")


def first_n(n):
    # A non-literal limit is still a bound.
    return df.limit(n).collect()


# Bounded or driver-sized: not reported.
by_constant = df.limit(N).collect()
fstring_limit = spark.sql(f"SELECT * FROM sales LIMIT {N}").collect()
fstring_limit_semicolon = spark.sql(f"SELECT * FROM sales\nLIMIT {N + 1};").collect()
one_row = spark.sql("SELECT count(*) FROM sales").collect()
range_rows = spark.range(10).collect()
literal_rows = spark.createDataFrame([(1,)], ["a"]).collect()
stats = df.describe().collect()
from_helper = helper().collect()

# Reported.
everything = df.collect()  # expect: ACT002
filtered = keep_active(df).collect()  # expect: ACT002
inner_limit = spark.sql(f"SELECT * FROM (SELECT * FROM sales LIMIT {N}) s JOIN o USING (k)").collect()  # expect: ACT002
placeholder_filter = spark.sql(f"SELECT * FROM sales WHERE id = {N}").collect()  # expect: ACT002
history = spark.sql("DESCRIBE HISTORY sales").collect()  # expect: ACT002
big_range = spark.range(1000000000).collect()  # expect: ACT002
joined = df.join(spark.table("o"), "k").collect()  # expect: ACT002
skipped_rows = df.offset(5).collect()  # expect: ACT002
