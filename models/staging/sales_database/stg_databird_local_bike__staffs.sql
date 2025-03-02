select
    staff_id,
    first_name,
    last_name,
    email,
    phone,
    active,
    store_id,
    manager_id
from {{ source('databird_local_bike', 'staffs') }}