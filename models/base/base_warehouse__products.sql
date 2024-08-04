-- base_warehouse__products.sql

SELECT
    id AS product_id,
    data->>'$.name' AS product_name,
    data->>'$.versions[*]' AS product_versions,
    data->>'$.countries[*]' AS product_countries,
FROM
    {{ source('warehouse', 'products') }}
