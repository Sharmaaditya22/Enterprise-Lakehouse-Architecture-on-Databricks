SELECT * FROM databrick_ws_dbproject.`03_gold`.d_sales_summary
where order_date between :DATE_RANGE.min and :DATE_RANGE.max 

SELECT * FROM databrick_ws_dbproject.`02_silver`.fact_orders
where order_date between :DATE_RANGE.min and :DATE_RANGE.max 

SELECT * FROM databrick_ws_dbproject.`02_silver`.dim_customers
where join_date between :DATE_RANGE.min and :DATE_RANGE.max 

SELECT item_name,
sum(quantity) as total_qty_sold
FROM databrick_ws_dbproject.`02_silver`.fact_order_item
where order_date between :DATE_RANGE.min and :DATE_RANGE.max
group BY item_name
order BY total_qty_sold desc
limit 10

SELECT * FROM databrick_ws_dbproject.`02_silver`.fact_order_item
where order_date between :DATE_RANGE.min and :DATE_RANGE.max

SELECT * FROM databrick_ws_dbproject.`03_gold`.d_restaurant_reviews

SELECT name as restaurant_name,
date(review_timestamp) as review_date,
count(distinct case when sentiment='positive' then review_id end) as positive_reviews_ct,
count(distinct case when sentiment='negative' then review_id end) as negative_reviews_ct,
count(distinct case when sentiment='neutral' then review_id end) as neutral_reviews_ct
FROM databrick_ws_dbproject.`02_silver`.fact_reviews f
join databrick_ws_dbproject.`02_silver`.dim_restaurants d ON f.restaurant_id=d.restaurant_id
group by 1,2
order by 1,2


SELECT restaurant_name,
rating_label,
rating_count
FROM databrick_ws_dbproject.`03_gold`.d_restaurant_reviews
lateral view stack(
    5,
    '5 Star',rating_5_count,s
    '4 Star',rating_4_count,
    '3 Star',rating_3_count,
    '2 Star',rating_2_count,
    '1 Star',rating_1_count
) as rating_label,rating_count

SELECT name as restaurant_name,
count(distinct case when f.issue_delivery='true' then review_id else null end) as issue_delivery_ct,
count(distinct case when f.issue_food_quality='true' then review_id else null end) as issue_food_quality_ct,
count(distinct case when f.issue_pricing='true' then review_id else null end) as issue_pricing_ct,
count(distinct case when f.issue_portion_size='true' then review_id else null end) as issue_portion_size_ct
FROM databrick_ws_dbproject.`02_silver`.fact_reviews f
join databrick_ws_dbproject.`02_silver`.dim_restaurants d ON f.restaurant_id = d.restaurant_id
group by 1

SELECT 
name as restaurant_name,
review_text,
review_timestamp
FROM databrick_ws_dbproject.`02_silver`.fact_reviews f JOIN
databrick_ws_dbproject.`02_silver`.dim_restaurants r ON f.restaurant_id = r.restaurant_id 
where f.sentiment='positive' and f.review_text is not null
order by review_timestamp desc

SELECT 
name as restaurant_name,
review_text,
review_timestamp
FROM databrick_ws_dbproject.`02_silver`.fact_reviews f JOIN
databrick_ws_dbproject.`02_silver`.dim_restaurants r ON f.restaurant_id = r.restaurant_id 
where f.sentiment='negative' and f.review_text is not null
order by review_timestamp desc