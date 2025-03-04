WITH get_sales_by_staff AS (
    SELECT 
        --order_id,
        staff_id,
        staff_name,
        store_id,
        store_name,
        DATE_TRUNC(order_date, MONTH) AS month,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_items) AS total_products_sold,
        SUM(total_revenue) AS total_sales
    FROM {{ref("int_sales_database__orders_aggregated")}}
    GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM get_sales_by_staff