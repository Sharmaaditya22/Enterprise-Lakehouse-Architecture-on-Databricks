# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

 items_schema = ArrayType(
        StructType(
            [
                StructField("item_id", StringType()),
                StructField("name", StringType()),
                StructField("category", StringType()),
                StructField("quantity", IntegerType()),
                StructField("unit_price", DecimalType(10, 2)),
                StructField("subtotal", DecimalType(10, 2)),
            ]
        )
    )

# COMMAND ----------

df_fact_order=spark.table('databrick_ws_dbproject.`01_bronze`.orders')\
    .withColumn('order_timestamp',to_timestamp(col('order_timestamp')))\
    .withColumn('order_date',to_date(col('order_timestamp')))\
    .withColumn('item_parsed',from_json(col('items'),items_schema))\
    .withColumn('item',explode(col('item_parsed')))\
    .select('order_id',col('item.item_id').alias('item_id'), "restaurant_id", "order_timestamp", "order_date", col("item.name").alias("item_name"), col("item.category").alias("category"),col("item.quantity").alias("quantity"),col("item.unit_price").cast("decimal(10,2)").alias("unit_price"),col("item.subtotal").cast("decimal(10,2)").alias("subtotal"))

df_fact_order.display()

# COMMAND ----------

