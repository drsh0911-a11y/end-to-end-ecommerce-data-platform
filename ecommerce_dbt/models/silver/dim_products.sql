SELECT
    product_id,
    product_name,
    category,
    price
FROM {{ source('raw_ecommerce', 'raw_products') }}