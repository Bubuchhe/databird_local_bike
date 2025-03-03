WITH get_sales_infos_aggregations_by_order AS (
    SELECT 
        order_id,
        SUM(quantity) AS total_items,
        SUM(total_price) AS total_revenue
    FROM {{ref('int_sales_database__order_details')}}
    GROUP BY order_id
),

get_orders_enriched AS (
    SELECT 
        o.order_id,
        o.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        o.store_id,
        s.store_name,
        o.staff_id,
        st.first_name || ' ' || st.last_name AS staff_name,
        o.order_date,
        o.shipped_date,
        o.order_status,
        DATE_DIFF(o.shipped_date, o.order_date, DAY) AS delivery_time
    FROM {{ref('int_sales_database__order_details')}} o
    LEFT JOIN {{ref('stg_databird_local_bike__customers')}} c ON o.customer_id = c.customer_id
    LEFT JOIN {{ref('stg_databird_local_bike__stores')}} s ON o.store_id = s.store_id
    LEFT JOIN {{ref('stg_databird_local_bike__staffs')}} st ON o.staff_id = st.staff_id
),

final AS (
    SELECT 
        oe.order_id,
        oe.customer_id,
        oe.customer_name,
        oe.store_id,
        oe.store_name,
        oe.staff_id,
        oe.staff_name,
        oe.order_date,
        oe.shipped_date,
        oe.order_status,
        oi.total_items,
        oi.total_revenue,
        oe.delivery_time
    FROM get_orders_enriched oe
    LEFT JOIN get_sales_infos_aggregations_by_order oi ON oe.order_id = oi.order_id
)
SELECT * FROM final