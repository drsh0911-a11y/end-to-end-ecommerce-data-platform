WITH staging AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    order_id,
    customer_id,
    order_timestamp,
    order_date,
    total_amount,
    payment_status,
    shipping_city,
    items_count,
    is_successful
FROM staging