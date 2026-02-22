#!/usr/bin/env python3

import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, row_number, broadcast
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

spark = (
    SparkSession.builder
    .appName("AdvancedSparkJob")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("ERROR")

customers = spark.createDataFrame(
    [(i, f"State_{i%5}") for i in range(1, 1001)],
    ["cust_id", "state"]
)

products = spark.createDataFrame(
    [(i, f"Category_{i%10}", random.randint(10, 100)) for i in range(1, 501)],
    ["prod_id", "category", "price"]
)

orders = spark.createDataFrame(
    [(i, random.randint(1, 1000), random.randint(1, 500), random.randint(1, 5))
     for i in range(1, 5001)],
    ["order_id", "cust_id", "prod_id", "qty"]
)

orders.persist(StorageLevel.MEMORY_ONLY)

joined = (
    orders
    .join(broadcast(products), "prod_id")
    .join(customers, "cust_id")
)

revenue_df = joined.withColumn("revenue", col("qty") * col("price"))

agg = (
    revenue_df
    .groupBy("state", "category")
    .agg(
        _sum("revenue").alias("total_revenue"),
        count("*").alias("total_orders")
    )
)

window_spec = Window.partitionBy("state").orderBy(col("total_revenue").desc())

ranked = agg.withColumn("rank", row_number().over(window_spec))

top_per_state = ranked.filter(col("rank") <= 3)

rdd_result = (
    top_per_state.rdd
    .map(lambda x: ((x["state"], x["category"]), x["total_revenue"]))
    .reduceByKey(lambda a, b: a + b)
)

final = rdd_result.collect()

for row in final:
    print(row)

spark.stop()