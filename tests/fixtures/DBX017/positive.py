from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

REGIONS = ["us", "eu", "apac", "latam"]

for region in REGIONS:

    @dp.table(name=f"orders_{region}")
    def orders(region=region):
        return spark.readStream.table("raw_orders").filter(f"region = '{region}'")  # expect: DBX017


@dp.table(name="payments")
def payments():
    return spark.readStream.table("bronze.events").where("type = 'payment'")  # expect: DBX017


@dp.table(name="refunds")
def refunds():
    return spark.readStream.table("bronze.events").where("type = 'refund'")


@dp.append_flow(target="all_signups")
def signups():
    return spark.readStream.table("BRONZE.EVENTS").where("type = 'signup'")


# Loop over config of unknown length: the fan-out pattern itself.
def build(conf):
    for t in conf["targets"]:

        @dp.table(name=f"kafka_{t}")
        def k():
            return (
                spark.readStream.format("kafka")  # expect: DBX017
                .option("kafka.bootstrap.servers", "h:9092")
                .option("subscribe", "clicks")
                .load()
            )


# A stored reader keyed by its topic option.
ORDERS_STREAM = spark.readStream.format("kafka").option("subscribe", "orders")

for n in ["a", "b", "c"]:

    @dp.table(name=f"orders_{n}")
    def per_n():
        return ORDERS_STREAM.load()  # expect: DBX017
