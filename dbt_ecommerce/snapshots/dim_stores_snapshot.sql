{% snapshot dim_stores_snapshot %}

{{
    config(
      target_schema='gold_layer',
      unique_key='store_id',
      strategy='check',
      check_cols=['store_name', 'city', 'region', 'store_type']
    )
}}

SELECT 
    store_id,
    MAX(store_name) AS store_name,
    MAX(city) AS city,
    MAX(region) AS region,
    MAX(store_type) AS store_type
FROM {{ ref('stg_reviews') }} 
WHERE store_id IS NOT NULL
GROUP BY store_id

{% endsnapshot %}