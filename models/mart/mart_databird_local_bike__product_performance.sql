WITH product_sales AS (
    SELECT
        product_id,
        product_name,
        category_name,
        brand_name,
        store_id,
        store_name,
        month,
        total_units_sold,
        total_products_sales as total_revenue,
        avg_price_sold,
        sales_percentage
    FROM {{ref('int_sales_database__sales_per_product')}}
),

product_rankings AS (
    SELECT
        product_id,
        product_name,
        category_name,
        brand_name,
        store_id,
        store_name,
        month,
        total_units_sold,
        total_revenue,
        avg_price_sold,
        sales_percentage,
        RANK() OVER (PARTITION BY month ORDER BY total_revenue DESC) as revenue_rank,
        RANK() OVER (PARTITION BY month, category_name ORDER BY total_revenue DESC) as category_revenue_rank,
        AVG(total_revenue) OVER (PARTITION BY product_id ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg_revenue
    FROM product_sales
)

SELECT
    product_id,
    product_name,
    category_name,
    brand_name,
    store_id,
    store_name,
    month,
    total_units_sold,
    total_revenue,
    avg_price_sold,
    sales_percentage,
    revenue_rank,
    category_revenue_rank,
    moving_avg_revenue,
    COALESCE(
        (total_revenue - LAG(total_revenue) OVER (PARTITION BY product_id ORDER BY month)) /
        NULLIF(LAG(total_revenue) OVER (PARTITION BY product_id ORDER BY month), 0) * 100,
        0
    ) as revenue_growth_percentage
FROM product_rankings