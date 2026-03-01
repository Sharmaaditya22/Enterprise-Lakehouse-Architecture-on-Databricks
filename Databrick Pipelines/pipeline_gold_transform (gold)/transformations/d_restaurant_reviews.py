from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.materialized_view(
    name='d_restaurant_reviews',
    table_properties={'quality': 'gold'}
)
def d_restaurant_reviews():
    df_reviews_stats=dp.read('`02_silver`.fact_reviews')\
    .groupBy(col('restaurant_id'))\
    .agg(countDistinct(col('review_id')).alias('total_reviews'),
        round(avg('rating'),2).alias('avg_rating'),
        sum(when(col('rating')==5,lit(1)).otherwise(lit(0))).alias('rating_5_count'),
        sum(when(col('rating')==4,lit(1)).otherwise(lit(0))).alias('rating_4_count'),
        sum(when(col('rating')==3,lit(1)).otherwise(lit(0))).alias('rating_3_count'),
        sum(when(col('rating')==2,lit(1)).otherwise(lit(0))).alias('rating_2_count'),
        sum(when(col('rating')==1,lit(1)).otherwise(lit(0))).alias('rating_1_count'),
        sum(when(col('sentiment')=='positive',lit(1)).otherwise(lit(0))).alias('sentiment_positive_count'),
        sum(when(col('sentiment')=='negative',lit(1)).otherwise(lit(0))).alias('sentiment_negative_count'),
        sum(when(col('sentiment')=='neutral',lit(1)).otherwise(lit(0))).alias('sentiment_neutral_count')
        )
    df_restaurant=dp.read('`02_silver`.dim_restaurants')

    df_restaurant_review=df_restaurant.join(df_reviews_stats,'restaurant_id','left')\
    .select(
        'restaurant_id',
        col('name').alias('restaurant_name'),
        'city',
        coalesce(col('total_reviews'),lit(0)).alias('total_reviews'),
        coalesce(col('avg_rating'),lit(0)).alias('avg_rating'),
        coalesce(col('rating_5_count'),lit(0)).alias('rating_5_count'),
        coalesce(col('rating_4_count'),lit(0)).alias('rating_4_count'),
        coalesce(col('rating_3_count'),lit(0)).alias('rating_3_count'),
        coalesce(col('rating_2_count'),lit(0)).alias('rating_2_count'),
        coalesce(col('rating_1_count'),lit(0)).alias('rating_1_count'),
        coalesce(col('sentiment_positive_count'),lit(0)).alias('sentiment_positive_count'),
        coalesce(col('sentiment_negative_count'),lit(0)).alias('sentiment_negative_count'),
        coalesce(col('sentiment_neutral_count'),lit(0)).alias('sentiment_neutral_count')
    )

    return df_restaurant_review