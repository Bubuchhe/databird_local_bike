select
    brand_id,
    brand_name
from {{ source('databird_local_bike', 'brands') }}