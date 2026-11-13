from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

for r in spark.sql("SHOW CATALOGS").collect():
    print(r["catalog"])

names = [t.tableName for t in spark.sql("SHOW TABLES IN db").collect()]

comments = {row["col_name"]: row["comment"] for row in spark.sql("DESCRIBE TABLE t").collect()}

props = spark.sql("SHOW TBLPROPERTIES t").collect()
for p in props:
    print(p.key)

for _, row in spark.sql("SHOW TBLPROPERTIES t").toPandas().iterrows():
    print(row)
