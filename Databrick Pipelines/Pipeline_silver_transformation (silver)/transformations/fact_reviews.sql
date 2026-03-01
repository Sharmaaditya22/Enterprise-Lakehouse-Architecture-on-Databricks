Create or Refresh streaming table databrick_ws_dbproject.`02_silver`.fact_reviews(
    CONSTRAINT valid_sentiment EXPECT (sentiment IN ('positive', 'neutral', 'negative')) ON VIOLATION DROP ROW,
    CONSTRAINT non_negative_rating EXPECT (rating >= 0) ON VIOLATION DROP ROW
)
with model_response as(
select
*,
ai_query(
  'databricks-gpt-oss-20b',
  CONCAT(
        'Analyze the following review and return ONLY a valid JSON object with this exact structure: ',
        '{"sentiment": "<positive/neutral/negative>", ',
        '"issue_delivery": <true/false>, ',
        '"issue_delivery_reason": "<reason or empty string>", ',
        '"issue_food_quality": <true/false>, ',
        '"issue_food_quality_reason": "<reason or empty string>", ',
        '"issue_pricing": <true/false>, ',
        '"issue_pricing_reason": "<reason or empty string>", ',
        '"issue_portion_size": <true/false>, ',
        '"issue_portion_size_reason": "<reason or empty string>"}. ',
        'Rules: sentiment must be exactly one of: positive, neutral, negative. ',
        'Each issue field is true/false only. ',
        'Each reason field should contain a brief explanation if the issue is true, otherwise empty string. ',
        'Review text: ', review_text
      )
) as analysis_jon
from stream(databrick_ws_dbproject.`01_bronze`.reviews)
)

select
review_id,
customer_id,
restaurant_id,
rating,
review_text,
analysis_jon,
get_json_object(analysis_jon,'$.sentiment') as sentiment,
get_json_object(analysis_jon,'$.issue_delivery') as issue_delivery,
get_json_object(analysis_jon,'$.issue_delivery_reason') as issue_delivery_reason,
get_json_object(analysis_jon,'$.issue_food_quality') as issue_food_quality,
get_json_object(analysis_jon,'$.issue_food_quality_reason') as issue_food_quality_reason,
get_json_object(analysis_jon,'$.issue_pricing') as issue_pricing,
get_json_object(analysis_jon,'$.issue_pricing_reason') as issue_pricing_reason,
get_json_object(analysis_jon,'$.issue_portion_size') as issue_portion_size,
get_json_object(analysis_jon,'$.issue_portion_size_reason') as issue_portion_size_reason,
review_timestamp
from model_response

