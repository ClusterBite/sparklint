from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

name = "orders"
names = ["orders", "customers"]

# Membership in the projected listing.
if name in [t.name for t in spark.catalog.listTables("sales")]:  # expect: DBX016
    pass

# Set comprehension and `not in`.
if name not in {t.name for t in spark.catalog.listTables("sales")}:  # expect: DBX016
    pass

# any() over a generator comparing the name.
exists = any(t.name == name for t in spark.catalog.listTables("sales"))  # expect: DBX016

# Filtering comprehension + len(); lower-cased name.
found = len([t for t in spark.catalog.listTables("sales") if t.name.lower() == name]) > 0  # expect: DBX016

# next() with a reversed comparison.
tbl = next((t for t in spark.catalog.listTables(dbName="sales") if name == t.name), None)  # expect: DBX016

# Schemas.
if "sales" in [d.name for d in spark.catalog.listDatabases()]:  # expect: DBX016
    pass

# for-loop with an if on the name.
for t in spark.catalog.listTables("sales"):  # expect: DBX016
    if t.name == name:
        print("found")

# for-loop whose name test is an elif branch.
for t in spark.catalog.listTables("sales"):  # expect: DBX016
    if t.isTemporary:
        continue
    elif t.name == name:
        print("found")

# SHOW TABLES collected, then a membership test on tableName.
if name in [r.tableName for r in spark.sql("SHOW TABLES IN sales").collect()]:  # expect: DBX016
    pass

# SHOW TABLES ... select ... collect, subscript access.
if name in [r["tableName"] for r in spark.sql("show tables from sales").select("tableName").collect()]:  # expect: DBX016
    pass

# SHOW TABLES filtered on tableName with a Column comparison, then counted.
n = spark.sql("SHOW TABLES IN sales").filter(F.col("tableName") == name).count()  # expect: DBX016

# SQL-string filter.
m = spark.sql("SHOW TABLES IN sales").where("tableName = 'orders'").collect()  # expect: DBX016

# Inside a loop: warning, the listing is fetched once per name.
for wanted in names:
    if wanted in [t.name for t in spark.catalog.listTables("sales")]:  # expect: DBX016
        print(wanted)


# Typed parameter.
def has_table(session: SparkSession, table: str) -> bool:
    return table in [t.name for t in session.catalog.listTables("sales")]  # expect: DBX016
