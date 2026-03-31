{{
    config(
        materialized='incremental',
        unique_key='order_id', 
        schema='platinum'
    )
}}

WITH stg_data AS (
    SELECT
        r.order_id,
        
        -- Lấy Khóa Giả (Surrogate Key)
        p.product_sk, 
        r.product_id AS original_product_id,
        
        s.store_sk, 
        r.store_id AS original_store_id, 
        
        r.user_id,
        r.payment_id,
        r.shipping_id,
        r.date_id,
        r.review_id,
        r.quantity,
        r.total_amount,
        r.rating_score,
        r.review_text
    FROM {{ ref('stg_reviews') }} r

    -- Kỹ thuật JOIN du hành thời gian tìm đúng ID Sản phẩm
    LEFT JOIN {{ ref('dim_products') }} p
        ON r.product_id = p.original_product_id
        AND r.date_id >= CAST(p.valid_from AS DATE)
        AND (r.date_id < CAST(p.valid_to AS DATE) OR p.valid_to IS NULL)

    -- Kỹ thuật JOIN du hành thời gian tìm đúng ID Cửa hàng
    LEFT JOIN {{ ref('dim_stores') }} s
        ON r.store_id = s.original_store_id
        AND r.date_id >= CAST(s.valid_from AS DATE)
        AND (r.date_id < CAST(s.valid_to AS DATE) OR s.valid_to IS NULL)
)

SELECT * FROM stg_data

{% if is_incremental() %}
  -- Lọc lấy data mới hơn ngày lớn nhất đang có trong bảng hiện tại
  WHERE date_id >= (SELECT COALESCE(MAX(date_id), '1900-01-01') FROM {{ this }})
{% endif %}