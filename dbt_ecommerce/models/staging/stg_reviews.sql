{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT * FROM {{ source('public', 'raw_ecommerce_reviews') }}
)
SELECT
    "orderID" AS order_id,
    "productName" AS product_name,
    "category" AS category,
    "sub_category" AS sub_category,
    "brand" AS brand,
    CAST("unitPrice" AS NUMERIC(10,2)) AS unit_price,
    CAST("quantity" AS INT) AS quantity,
    CAST("totalAmount" AS NUMERIC(12,2)) AS total_amount,
    
    "storeID" AS store_id,
    "storeName" AS store_name,
    "city" AS city,
    "region" AS region,
    "storeType" AS store_type,
    
    "paymentID" AS payment_id,
    "paymentMethod" AS payment_method,
    "paymentStatus" AS payment_status,
    
    "shippingID" AS shipping_id,
    "shippingMethod" AS shipping_method,
    "carrierName" AS carrier_name,
    "shippingStatus" AS shipping_status,

    md5(COALESCE("reviewerID", '') || COALESCE("asin", '') || CAST("unixReviewTime" AS VARCHAR)) AS review_id,
    "reviewerID" AS user_id,
    "reviewerName" AS user_name,
    "asin" AS product_id,
    CAST(TO_TIMESTAMP("unixReviewTime") AS DATE) AS date_id,
    CAST("overall" AS NUMERIC(3,1)) AS rating_score,
    "reviewText" AS review_text
FROM raw_data