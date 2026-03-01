from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark import pipelines as dp

@dp.materialized_view(
    name='customer360',
    table_properties={'quality':'gold'}
)
def customer360():
    df_orders=dp.read('`02_silver`.fact_orders')

    df_order_stats=df_orders.groupBy('customer_id')\
    .agg(
        countDistinct(col('order_id')).alias('total_orders'),
        sum('total_amount').alias('lifetime_spend'),
        round(avg(col('total_amount')),2).alias('avg_order_amount'),
        max('order_date').alias('last_order_date')
    )\
    .withColumn('loyalty_tier',
                when(col('lifetime_spend')>=5000,'Platinum')
                .when(col('lifetime_spend')>=2000,'gold')
                .when(col('lifetime_spend')>=1000,'silver')
                .otherwise('bronze')
    )

    df_review=dp.read('`02_silver`.fact_reviews')

    df_review_stats=df_review.groupBy('customer_id')\
    .agg(
        countDistinct(col('review_id')).alias('total_reviews'),
        round(avg(col('rating')),2).alias('avg_rating_given')
    )

    window=Window.partitionBy(col('customer_id')).orderBy(col('order_cnt').desc())
    df_restaurant=dp.read('`02_silver`.dim_restaurants')
    df_fav_restaurant=df_orders.join(df_restaurant,'restaurant_id')\
    .groupBy('customer_id','name')\
    .agg(
        count(col('order_id')).alias('order_cnt')
    )\
    .withColumn('rank',row_number().over(window))\
    .filter(col('rank')==1)\
    .drop('rank','order_cnt').select('customer_id',col('name').alias('restaurant_name'))

    window1=Window.partitionBy(col('customer_id')).orderBy(col('item_quantity').desc())
    df_fact_order_items=dp.read('`02_silver`.fact_order_item')

    df_fav_item=df_orders.join(df_fact_order_items,'order_id')\
    .groupBy('customer_id','item_name')\
    .agg(
        count(col('quantity')).alias('item_quantity')
    )\
    .withColumn('rank',row_number().over(window1))\
    .filter(col('rank')==1)\
    .drop('rank','item_quantity').select('customer_id',col('item_name').alias('favorite_item'))

    df_customer=dp.read('`02_silver`.dim_customers')

    df_c360=df_customer.join(df_order_stats,'customer_id','left')\
    .join(df_review_stats,'customer_id','left')\
    .join(df_fav_restaurant,'customer_id','left')\
    .join(df_fav_item,'customer_id','left')\
    .select(
        # customer details
        'customer_id',
        col('name').alias('customer_name'),
        'email',
        'city',
        'join_date',
        # order stats
        'loyalty_tier',
        coalesce(col('total_orders'),lit(0)).alias('total_orders'),
        coalesce(col('lifetime_spend'),lit(0)).cast('decimal(10,2)').alias('lifetime_spend'),
        coalesce(col('avg_order_amount'),lit(0)).cast('decimal(10,2)').alias('avg_order_value'),
        col('last_order_date'),
        # Review stats
        coalesce(col('total_reviews'),lit(0)).cast('decimal(10,2)').alias('total_reviews'),
        coalesce(col('avg_rating_given'),lit(0)).cast('decimal(10,2)').alias('avg_rating_given'),
        # Favorite metrics
        'restaurant_name',
        'favorite_item',
        
        when(col('lifetime_spend')>=5000,True).otherwise(False).alias('is_VIP')
        )
    
    return df_c360




