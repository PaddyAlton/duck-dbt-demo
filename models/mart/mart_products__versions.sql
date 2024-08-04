-- mart_products__versions.sql
-- Bridge representing the many-to-many product/version relationship

SELECT
    product_id,
    product_version,
FROM
    {{ ref('intm_products__versions') }}
