WITH get_order_aggregations AS (
    SELECT 
        order_id,
        store_id,
        order_date,
        SUM(total_items) AS total_products_sold,
        SUM(total_revenue) AS total_sales
    FROM {{ref('int_sales_database__orders_aggregated')}}
    GROUP BY 1, 2, 3
),

get_store_scoped_order_metrics AS (
    SELECT 
        store_id,
        order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        AVG(delivery_time) AS avg_delivery_time
    FROM {{ref('int_sales_database__orders_aggregated')}}
    GROUP BY 1, 2
),

get_store_scoped_stock_metrics AS (
    SELECT 
        store_id,
        SUM(quantity) AS total_stock_available
    FROM {{ref('stg_databird_local_bike__stocks')}}
    GROUP BY store_id
),

final AS (
    SELECT 
        s.store_id,
        s.store_name,
        s.city,
        s.state,
        DATE_TRUNC(om.order_date, MONTH) AS order_date,
        COALESCE(om.total_orders, 0) AS total_orders,
        COALESCE(om.avg_delivery_time, 0) AS avg_delivery_time,
        COALESCE(sm.total_stock_available, 0) AS total_stock_available,
        SUM(COALESCE(oi.total_sales, 0)) AS total_sales,
        SUM(COALESCE(oi.total_products_sold, 0)) AS total_products_sold,
        RANK() OVER (ORDER BY SUM(COALESCE(oi.total_sales, 0)) DESC) AS sales_rank
    FROM {{ref('stg_databird_local_bike__stores')}} s
    LEFT JOIN get_store_scoped_order_metrics om USING(store_id)
    LEFT JOIN get_order_aggregations oi USING(store_id)
    LEFT JOIN get_store_scoped_stock_metrics sm USING(store_id)
    GROUP BY 1,2,3,4,5,6,7,8
)

SELECT * FROM final
