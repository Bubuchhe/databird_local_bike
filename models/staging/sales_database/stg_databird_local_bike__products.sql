select
    product_id,
    product_name,
    brand_id,
    category_id, 
    model_year,
    CAST(list_price as numeric) as unit_price
from {{ source('databird_local_bike', 'products') }}