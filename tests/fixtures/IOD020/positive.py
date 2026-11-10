from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Four Z-order keys in SQL
spark.sql("OPTIMIZE lake.events ZORDER BY (user_id, event_type, country, device)")  # expect: IOD020

# lower case, partition filter, backticked names
spark.sql("optimize lake.clicks where dt = '2024-01-01' zorder by (`user id`, page, ref, ua, ip)")  # expect: IOD020

# no parentheses around the list
spark.sql("OPTIMIZE lake.orders ZORDER BY customer_id, sku, store_id, channel")  # expect: IOD020

# multi-line statement with a comment, keyword argument
spark.sql(  # expect: IOD020
    sqlQuery="""
    -- nightly maintenance
    OPTIMIZE lake.sales
    ZORDER BY (
        region,
        product_id,
        customer_id,
        sold_at
    )
    """
)

# f-string table name
table = "lake.views"
spark.sql(f"OPTIMIZE {table} ZORDER BY (a, b, c, d)")  # expect: IOD020

# statement bound to a name once
MAINT = "OPTIMIZE lake.sessions ZORDER BY (a, b, c, d, e, f)"
spark.sql(MAINT)  # expect: IOD020

# delta-spark builder: positional names
DeltaTable.forName(spark, "lake.events").optimize().executeZOrderBy("a", "b", "c", "d")  # expect: IOD020

# one list argument, after a partition filter
dt = DeltaTable.forPath(spark, "/mnt/lake/events")
dt.optimize().where("dt = '2024-01-01'").executeZOrderBy(["a", "b", "c", "d"])  # expect: IOD020

# a name bound to one column plus literals
first = "user_id"
dt.optimize().executeZOrderBy(first, "b", "c", "d")  # expect: IOD020


# inside a function with a typed session
def maintain(s: SparkSession) -> None:
    s.sql("OPTIMIZE t ZORDER BY (w, x, y, z)")  # expect: IOD020
