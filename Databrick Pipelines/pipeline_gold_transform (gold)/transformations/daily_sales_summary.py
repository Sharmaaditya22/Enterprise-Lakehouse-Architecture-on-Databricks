from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

@dp.materialized_view(
    name='d_sales_summary',
    partition_cols=['order_date'],
    table_properties={'quality':'gold'}
)
def d_sales_summary():
    df_daily_agg=dp.read('`02_silver`.fact_orders')\
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
    
    return df_daily_agg