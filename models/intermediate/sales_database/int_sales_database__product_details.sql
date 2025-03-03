select 
    product_id,
    product_name,
    category_name,
    category_id,
    brand_name,
    brand_id,
    model_year,
    unit_price
from {{ref('stg_databird_local_bike__products')}}
inner join {{ref('stg_databird_local_bike__categories')}} USING(category_id)
inner join {{ref('stg_databird_local_bike__brands')}} USING(brand_id)