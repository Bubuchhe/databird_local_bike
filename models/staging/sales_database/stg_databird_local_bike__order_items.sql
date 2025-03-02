select
    CONCAT(order_id, '_', product_id) AS order_item_id,
    order_id,
    product_id,
    item_id,
    quantity,
    list_price AS unit_price,
    discount,
    ROUND(list_price * (1 - discount / 100) * quantity, 2) AS total_price
from {{ source('databird_local_bike', 'order_items') }}