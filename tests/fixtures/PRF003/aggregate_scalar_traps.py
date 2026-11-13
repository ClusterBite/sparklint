from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

funds = spark.read.table("fund_trans_silver")

# A generator over an aggregate is several rows; still a cartesian product.
exploded = funds.select(F.explode(F.collect_list("payer")).alias("p"))
pairs = funds.crossJoin(exploded)  # expect: PRF003
# A row-wise function of a plain column is one row per input row.
dates = funds.select(F.to_date("datetime").alias("d"))
all_pairs = funds.crossJoin(dates)  # expect: PRF003


# A parameter with both DataFrame and pandas-looking usage keeps the df heuristic.
def mixed(df, other):
    loc = df.loc
    return df.withColumn("a", F.lit(1)).crossJoin(other), loc  # expect: PRF003
