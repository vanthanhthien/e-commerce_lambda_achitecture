{% snapshot dim_products_snapshot %}

{{
    config(
      target_schema='gold_layer',
      unique_key='product_id',
      strategy='check',
      check_cols=['product_name', 'category', 'sub_category', 'brand', 'unit_price']
    )
}}

SELECT 
    product_id,
    MAX(product_name) AS product_name,
    MAX(category) AS category,
    MAX(sub_category) AS sub_category,
    MAX(brand) AS brand,
    MAX(unit_price) AS unit_price
FROM {{ ref('stg_reviews') }} 
WHERE product_id IS NOT NULL
GROUP BY product_id

{% endsnapshot %}