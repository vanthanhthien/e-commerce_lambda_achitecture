WITH stg_reviews AS (
    SELECT * FROM {{ ref('stg_reviews') }}
)

SELECT DISTINCT
    date_id,
    EXTRACT(YEAR FROM date_id) AS review_year,
    EXTRACT(MONTH FROM date_id) AS review_month,
    EXTRACT(DAY FROM date_id) AS review_day,
    TO_CHAR(date_id, 'Day') AS day_of_week
FROM stg_reviews
WHERE date_id IS NOT NULL