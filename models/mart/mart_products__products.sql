-- mart_products__products.sql
-- Dimension table representing products and their properties

SELECT
    product_id,
    product_name,
FROM
    {{ ref('intm_products__products') }}
