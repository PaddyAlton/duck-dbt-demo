-- intm_products__products.sql
-- Dimension table representing products and their properties

SELECT
    product_id,
    product_name,
FROM
    {{ ref('base_warehouse__products') }}
