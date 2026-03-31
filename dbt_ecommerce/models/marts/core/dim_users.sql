WITH stg_reviews AS (
    SELECT * FROM {{ ref('stg_reviews') }}
)

SELECT 
    user_id,
    MAX(user_name) AS user_name, -- Lấy tên mới nhất nếu 1 user đổi tên nhiều lần
    COUNT(review_id) AS total_reviews_written,
    MIN(date_id) AS first_review_date,
    MAX(date_id) AS last_review_date
FROM stg_reviews
WHERE user_id IS NOT NULL
GROUP BY user_id