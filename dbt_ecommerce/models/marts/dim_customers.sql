WITH staging AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    customer_id,
    COUNT(order_id) AS total_orders,
    SUM(total_amount) AS total_lifetime_value,
    MAX(order_timestamp) AS last_order_date
FROM staging
GROUP BY customer_id