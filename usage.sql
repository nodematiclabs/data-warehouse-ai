CREATE OR REPLACE TABLE `demonstration.reviews_en` AS (
  SELECT 
    review_id,
    product_id,
    customer_id,
    rating,
    demonstration.llm(
      CONCAT(
        'Turn this text into a product review.  Text: ', demonstration.translate(review_text), '  Product Review: '
      )
    ) as review_text,
    review_date,
  FROM `demonstration.reviews`
  WHERE review_id < 100
)