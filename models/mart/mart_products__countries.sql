-- mart_products__countries.sql
-- Bridge representing the many-to-many product/country relationship

SELECT
    product_id,
    product_country,
FROM
    {{ ref('intm_products__countries') }}
