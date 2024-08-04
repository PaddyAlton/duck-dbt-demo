-- intm_products__versions.sql
-- Bridge representing the many-to-many product/version relationship

SELECT
    product_id,
    UNNEST(product_versions)::INTEGER AS product_version,
FROM
    {{ ref('base_warehouse__products') }}
