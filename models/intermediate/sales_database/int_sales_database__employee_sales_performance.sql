WITH get_sales_by_staff AS (
    SELECT 
        --order_id,
        staff_id,
        DATE_TRUNC(order_date, MONTH) AS month,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity) AS total_products_sold,
        SUM(total_price) AS total_sales
    FROM {{ref("int_sales_database__order_details")}}
    GROUP BY 1, 2
),

final AS (
    SELECT 
        st.staff_id,
        st.first_name || ' ' || st.last_name AS employee_name,
        st.store_id,
        s.store_name,
        ss.month,
        COALESCE(ss.total_orders, 0) AS total_orders,
        COALESCE(ss.total_sales, 0) AS total_sales,
        COALESCE(ss.total_products_sold, 0) AS total_products_sold,
        --RANK() OVER (ORDER BY COALESCE(ss.total_sales, 0) DESC) AS sales_rank
    FROM {{ref("stg_databird_local_bike__staffs")}} st
    INNER JOIN get_sales_by_staff ss ON st.staff_id = ss.staff_id
    INNER JOIN {{ref("stg_databird_local_bike__stores")}} s ON st.store_id = s.store_id
)

SELECT * FROM final