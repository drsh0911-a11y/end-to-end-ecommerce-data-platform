WITH raw_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date::TIMESTAMP AS order_timestamp,
        total_amount,
        payment_status,
        shipping_city,
        items_count
    FROM {{ source('raw', 'daily_transactions') }}
)

SELECT
    order_id,
    customer_id,
    order_timestamp,
    DATE(order_timestamp) AS order_date,
    total_amount,
    payment_status,
    shipping_city,
    items_count,
    CASE 
        WHEN payment_status = 'COMPLETED' THEN TRUE 
        ELSE FALSE 
    END AS is_successful
FROM raw_orders