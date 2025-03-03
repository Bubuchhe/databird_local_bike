with get_order_details AS (
  select
    product_id,
    quantity,
    unit_price,
    discount,
    order_date, 
    total_price
  from {{ref('int_sales_database__order_details')}}
)
, get_product_sales_aggregations AS (
  select 
    product_id,
    DATE_TRUNC(order_date, MONTH) AS month,
    SUM(quantity) AS total_units_sold,
    SUM(total_price) AS total_sales,
    AVG(unit_price * (1 - discount)) AS avg_price_sold
  from get_order_details 
  group by 1,2
)
, get_product_scoped_sales_infos AS (
  select
    sa.product_id,
    p.product_name,
    p.category_name,
    p.brand_name,
    sa.month,
    sa.total_units_sold,
    sa.total_sales,
    sa.avg_price_sold,
    sa.total_sales * 100.0 / sum(sa.total_sales) over(partition by sa.month) as sales_percentage
  from get_product_sales_aggregations sa
  inner join {{ref('int_sales_database__product_details')}} p USING(product_id)
)
select * from get_product_scoped_sales_infos