WITH get_order_details_infos AS (
    SELECT 
        product_id,
        quantity,
        unit_price,
        discount,
        order_date
    FROM  {{ref('int_sales_database__order_details')}}
),

get_category_scoped_sales_agg AS (
    SELECT
        p.category_id,
        p.category_name,
        DATE_TRUNC(oi.order_date, MONTH) AS month,
        SUM(oi.quantity) AS total_units_sold,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount)) AS total_sales,
        AVG(oi.unit_price * (1 - oi.discount)) AS avg_price_sold
    FROM get_order_details_infos oi
    INNER JOIN {{ref('int_sales_database__product_details')}} p ON oi.product_id = p.product_id
    GROUP BY 1, 2, 3
),

get_category_details_scoped_sales AS (
    SELECT 
        sa.category_id,
        sa.category_name,
        sa.month,
        sa.total_units_sold,
        sa.total_sales,
        sa.avg_price_sold,
        sa.total_sales * 100.0 / SUM(sa.total_sales) OVER(PARTITION BY sa.month) AS sales_percentage
    FROM get_category_scoped_sales_agg sa
)

SELECT * FROM get_category_details_scoped_sales