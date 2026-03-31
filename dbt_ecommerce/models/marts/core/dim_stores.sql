SELECT
    dbt_scd_id AS store_sk, 
    store_id AS original_store_id, 
    store_name,
    city,
    region,
    store_type,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN 'Active' ELSE 'Expired' END AS status
FROM {{ ref('dim_stores_snapshot') }}