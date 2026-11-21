from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import input_file_block_length

spark = SparkSession.builder.getOrCreate()
df = spark.read.json("/landing")

# The block offset / length functions are unavailable on Unity Catalog too.
a = df.withColumn("off", F.input_file_block_start())  # expect: DBX003 ~"input_file_block_start()" ~"_metadata.file_block_start" help~"_metadata.file_block_start"
b = df.withColumn("len", input_file_block_length())  # expect: DBX003 ~"_metadata.file_block_length"
c = df.selectExpr("*", "input_file_block_start() AS off")  # expect: DBX003 ~"input_file_block_start()"
d = spark.sql("SELECT input_file_name(), input_file_block_length() FROM json.`/x`")  # expect: DBX003, DBX003
e = df.withColumn("src", F.input_file_name())  # expect: DBX003[warning] ~"no longer supported from 17.3 LTS" ~"_metadata.file_path" help~"file-source DataFrame"

# F.call_function calls the SQL function by name (Spark 3.5+).
f = df.withColumn("src", F.call_function("input_file_name"))  # expect: DBX003 ~"input_file_name()"
FN = "INPUT_FILE_BLOCK_START"
g = df.withColumn("off", F.call_function(FN))  # expect: DBX003 ~"input_file_block_start()"

# Not reported: other functions called by name, the metadata column itself, and
# local functions of the same name.
h = df.withColumn("u", F.call_function("upper", F.col("name")))
i = df.select("*", "_metadata.file_block_start")


def input_file_block_start():
    return 0


j = input_file_block_start()
k = df.withColumn("u", F.call_function(df.columns[0]))
