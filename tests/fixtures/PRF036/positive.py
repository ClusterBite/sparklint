from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
orders = spark.read.table("orders")
customers = spark.read.table("customers")

# DataFrame hint, any case, with and without parameters.
j1 = orders.join(customers.hint("shuffle_replicate_nl"), "customer_id")  # expect: PRF036
j2 = orders.hint("SHUFFLE_REPLICATE_NL").join(customers, "customer_id")  # expect: PRF036
j3 = orders.join(customers.hint(" Shuffle_Replicate_NL "), "customer_id")  # expect: PRF036
j4 = orders.join(
    customers.filter(F.col("active")).hint(  # expect: PRF036
        name="shuffle_replicate_nl"
    ),
    on="customer_id",
)

# The hint name bound once to a constant.
NL = "shuffle_replicate_nl"
j5 = orders.hint(NL).join(customers, "customer_id")  # expect: PRF036

# SQL hint comments.
q1 = spark.sql(  # expect: PRF036
    """
    SELECT /*+ SHUFFLE_REPLICATE_NL(c) */ o.*, c.name
    FROM orders o JOIN customers c ON o.customer_id = c.customer_id
    """
)
q2 = spark.sql("select /*+ broadcast(r), shuffle_replicate_nl(c) */ * from o join c on o.k = c.k")  # expect: PRF036
QUERY = "SELECT /*+ shuffle_replicate_nl(b) */ * FROM a JOIN b ON a.k = b.k"
q3 = spark.sql(QUERY)  # expect: PRF036
table = "events"
q4 = spark.sql(f"SELECT /*+ SHUFFLE_REPLICATE_NL(e) */ * FROM {table} e JOIN d ON e.k = d.k")  # expect: PRF036
q5 = spark.sql(sqlQuery="SELECT /*+ SHUFFLE_REPLICATE_NL(d) */ * FROM e JOIN d ON e.k = d.k")  # expect: PRF036


def enrich(df: DataFrame, dims: DataFrame) -> DataFrame:
    return df.join(dims.hint("shuffle_replicate_nl"), "k")  # expect: PRF036


def run(session: SparkSession, days):
    for d in days:
        session.sql(f"SELECT /*+ SHUFFLE_REPLICATE_NL(x) */ * FROM t_{d} JOIN x ON t.k = x.k")  # expect: PRF036
