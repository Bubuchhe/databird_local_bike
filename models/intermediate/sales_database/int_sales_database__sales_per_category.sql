WITH get_category_scoped_sales_agg AS (
    SELECT
        category_id,
        category_name,
        month,
        SUM(avg_price_sold * total_units_sold) / SUM(total_units_sold) AS avg_category_price,
        SUM(total_units_sold) AS total_units_sold,
        SUM(total_products_sales) AS total_sales
    FROM {{ref('int_sales_database__sales_per_product')}}
    GROUP BY 1,2,3
),

get_category_details_scoped_sales AS (
    SELECT 
        sa.category_id,
        sa.category_name,
        sa.month,
        sa.total_units_sold,
        sa.total_sales,
        sa.avg_category_price,
        sa.total_sales * 100.0 / SUM(sa.total_sales) OVER(PARTITION BY sa.month) AS sales_percentage
    FROM get_category_scoped_sales_agg sa
)

SELECT * FROM get_category_details_scoped_sales