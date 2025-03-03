select
    order_id,
    customer_id,
    store_id,
    staff_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(required_date AS DATE) AS required_date,
    CASE 
        WHEN shipped_date = 'NULL' OR shipped_date IS NULL THEN NULL
        ELSE CAST(shipped_date AS DATE)
    END AS shipped_date,
    order_status
from {{ source('databird_local_bike', 'orders') }}