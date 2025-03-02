select
    order_id,
    customer_id,
    store_id,
    staff_id,
    order_date,
    required_date,
    shipped_date,
    order_status
from {{ source('databird_local_bike', 'orders') }}