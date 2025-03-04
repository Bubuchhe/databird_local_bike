    select
    CONCAT(store_id, '_', product_id) AS stock_id,
    store_id,
    product_id,
    quantity
from {{ source('databird_local_bike', 'stocks') }}