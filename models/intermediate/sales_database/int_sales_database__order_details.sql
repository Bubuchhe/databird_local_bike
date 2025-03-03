with get_order_details as (
  select
    oi.order_id,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    oi.discount,
    o.order_date,
    o.shipped_date,
    o.store_id, 
    o.customer_id,
    o.staff_id,
    o.order_status,
    oi.total_price,
    DATE_DIFF(o.shipped_date, o.order_date, DAY) AS delivery_time
  from {{ref('stg_databird_local_bike__order_items')}} oi
  inner join {{ref('stg_databird_local_bike__orders')}} o USING(order_id)
)

select * from get_order_details