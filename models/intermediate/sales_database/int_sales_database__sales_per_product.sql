with get_product_sales_aggregations AS (
  select 
    product.product_id,
    store_id, 
    store_name,
    DATE_TRUNC(order_date, MONTH) AS month,
    SUM(product.product_quantity) AS total_units_sold,
    SUM(product.product_price * product.product_quantity) AS total_products_sales,
    AVG(product.product_price * (1 - product.product_discount)) AS avg_price_sold
  from {{ref('int_sales_database__orders_aggregated')}}, UNNEST(products) product
  group by 1,2,3,4
)
, get_product_scoped_sales_infos AS (
  select
    sa.product_id,
    p.product_name,
    p.category_name,
    p.category_id,
    p.brand_name,
    sa.store_id,
    sa.store_name,
    sa.month,
    sa.total_units_sold,
    sa.total_products_sales,
    sa.avg_price_sold,
    sa.total_products_sales * 100.0 / sum(sa.total_products_sales) over(partition by sa.month) as sales_percentage
  from get_product_sales_aggregations sa
  inner join {{ref('int_sales_database__product_details')}} p USING(product_id)
)
select * from get_product_scoped_sales_infos