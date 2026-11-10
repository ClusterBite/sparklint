from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

name = "orders"
names = ["orders", "customers"]

# The recommended single lookups.
ok1 = spark.catalog.tableExists("sales.orders")
ok2 = spark.catalog.databaseExists("sales")

# Listed once, then many lookups: fine.
existing = {t.name for t in spark.catalog.listTables("sales")}
for wanted in names:
    if wanted in existing:
        print(wanted)

# The full listing is genuinely needed.
all_names = [t.name for t in spark.catalog.listTables("sales")]
for t in spark.catalog.listTables("sales"):
    print(t.name, t.tableType)

# Filtering on other fields, or with != / in / startswith.
views = [t.name for t in spark.catalog.listTables("sales") if t.tableType == "VIEW"]
temps = [t for t in spark.catalog.listTables("sales") if t.isTemporary == True]  # noqa: E712
others = [t.name for t in spark.catalog.listTables("sales") if t.name != name]
chosen = [t for t in spark.catalog.listTables("sales") if t.name in names]
staging = [t.name for t in spark.catalog.listTables("sales") if t.name.startswith("stg_")]

# Pattern arguments filter in the metastore.
pat = [t.name for t in spark.catalog.listTables("sales", "ord*") if t.name == name]
pat2 = any(t.name == name for t in spark.catalog.listTables("sales", pattern="ord*"))
if name in [r.tableName for r in spark.sql("SHOW TABLES IN sales LIKE 'ord*'").collect()]:
    pass

# SHOW TABLES filtered on other columns.
perm = spark.sql("SHOW TABLES IN sales").filter(F.col("isTemporary") == False).collect()  # noqa: E712
perm2 = spark.sql("SHOW TABLES IN sales").where("isTemporary = false").collect()
neq = spark.sql("SHOW TABLES IN sales").where("tableName != 'orders'").collect()

# Other SHOW statements and plain queries.
cols = [r.col_name for r in spark.sql("SHOW COLUMNS IN sales.orders").collect() if r.col_name == "id"]
rows = spark.sql("SELECT tableName FROM meta").filter(F.col("tableName") == name).count()


# User class with listTables: not the Spark catalog.
class Registry:
    def listTables(self, db):
        return []


reg = Registry()
if name in [t.name for t in reg.listTables("sales")]:
    pass

# Plain Python lists of tables.
tables = [{"name": "orders"}]
hit = any(t["name"] == name for t in tables)

# Tuple targets are not tracked.
pairs = [(i, t) for i, t in enumerate(spark.catalog.listTables("sales")) if t.name == name]

# Suppressed.
if name in [t.name for t in spark.catalog.listTables("sales")]:  # noqa: DBX016
    pass
