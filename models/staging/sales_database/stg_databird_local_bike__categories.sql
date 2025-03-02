select
    category_id,
    category_name
from {{ source('databird_local_bike', 'categories') }}