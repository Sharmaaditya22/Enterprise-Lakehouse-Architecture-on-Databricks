# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Cell 2
df_daily_agg=spark.table('`02_silver`.fact_orders')\
    .groupBy('order_date')\
    .agg(countDistinct(col('order_id')).alias('total_orders'),sum(col('total_amount')).cast('decimal(10,2)').alias('total_revenue'),avg(col('total_amount')).alias('avg_order_value'),
         countDistinct(col('customer_id')).alias('unique_customers'),countDistinct(col('restaurant_id')).alias('unique_restaurant'),countDistinct(
             when(col('order_type')=='dine_in',col('order_id')).otherwise(None)
         ).alias('dine_in_orders'),
         countDistinct(
             when(col('order_type')=='take_away',col('order_id')).otherwise(None)
         ).alias('take_away_orders'),
         countDistinct(
             when(col('order_type')=='delivery',col('order_id')).otherwise(None)
         ).alias('delivery_orders'))
df_daily_agg.display()

# COMMAND ----------

