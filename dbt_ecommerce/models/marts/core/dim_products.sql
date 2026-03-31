SELECT
    dbt_scd_id AS product_sk, 
    product_id AS original_product_id,
    product_name,
    category,
    sub_category,
    brand,
    unit_price,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN 'Active' ELSE 'Expired' END AS status
FROM {{ ref('dim_products_snapshot') }}