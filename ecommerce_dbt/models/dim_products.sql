-- Dòng config này ra lệnh cho dbt: "Hãy tạo cho tôi một cái Bảng (table) vật lý trong database"
{{ config(materialized='table') }}

WITH source_data AS (
    SELECT 1 AS product_id, 'Laptop Dell XPS 15' AS product_name, 'Electronics' AS category, 1500 AS price
    UNION ALL
    SELECT 2, 'Chuột Logitech G502', 'Accessories', 50
    UNION ALL
    SELECT 3, 'Bàn phím cơ Keychron K8', 'Accessories', 100
    UNION ALL
    SELECT 4, 'Màn hình LG UltraGear', 'Electronics', 350
)

SELECT 
    product_id,
    product_name,
    category,
    price,
    CURRENT_TIMESTAMP AS created_at -- Tự động lấy thời gian hiện tại lúc tạo bảng
FROM source_data