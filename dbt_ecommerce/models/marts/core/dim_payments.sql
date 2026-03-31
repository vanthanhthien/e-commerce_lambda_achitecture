SELECT 
    payment_id,
    MAX(payment_method) AS payment_method,
    MAX(payment_status) AS payment_status
FROM {{ ref('stg_reviews') }} 
WHERE payment_id IS NOT NULL
GROUP BY payment_id