SELECT
    customer_id,
    first_name,
    last_name,
    email,
    signup_date,
    DATEDIFF(day, signup_date, GETDATE()) AS account_age_days
FROM {{ source('raw_ecommerce', 'raw_customers') }}