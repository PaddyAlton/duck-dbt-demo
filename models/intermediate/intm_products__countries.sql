-- intm_products__countries.sql
-- Bridge representing the many-to-many product/country relationship

SELECT
    product_id,
    UNNEST(product_countries) AS product_country,
FROM
    {{ ref('base_warehouse__products') }}
