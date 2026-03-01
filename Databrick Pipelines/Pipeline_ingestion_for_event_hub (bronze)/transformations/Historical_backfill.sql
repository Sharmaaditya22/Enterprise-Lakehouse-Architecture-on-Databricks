CREATE FLOW append_historical_data
AS INSERT INTO `01_bronze`.orders BY NAME
SELECT 
  order_id,
  CAST(timestamp AS STRING) AS order_timestamp,
  restaurant_id,
  customer_id,
  order_type,
  items,
  CAST(total_amount AS DOUBLE) AS total_amount,
  payment_method,
  order_status
FROM STREAM(`01_bronze`.historical_orders);