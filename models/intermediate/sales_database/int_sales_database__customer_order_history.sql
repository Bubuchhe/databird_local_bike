WITH customer_order_history AS (
    SELECT
        customer_id,
        customer_name,
        order_id,
        order_date,
        total_items,
        total_revenue,
        products,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as order_sequence,
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) as previous_order_date
    FROM {{ref('int_sales_database__orders_aggregated')}}
)

,customer_product_preferences AS (
    SELECT
        customer_id,
        p.category_id,
        p.category_name,
        COUNT(DISTINCT coh.order_id) as category_purchase_count,
        SUM(prod.product_quantity) as total_items_in_category,
        SUM(prod.product_quantity * prod.product_price * (1 - prod.product_discount)) as total_spent_in_category
    FROM customer_order_history coh
    CROSS JOIN UNNEST(products) as prod
    INNER JOIN {{ref('int_sales_database__product_details')}} p 
        ON prod.product_id = p.product_id
    GROUP BY 1, 2, 3
)

,customer_metrics AS (
    SELECT
        customer_id,
        customer_name,
        COUNT(DISTINCT order_id) as lifetime_orders,
        SUM(total_items) as lifetime_items,
        SUM(total_revenue) as lifetime_revenue,
        MIN(order_date) as first_order_date,
        MAX(order_date) as last_order_date,
        AVG(DATE_DIFF(order_date, previous_order_date, DAY)) as avg_days_between_orders,
        AVG(total_revenue) as avg_order_value
    FROM customer_order_history
    GROUP BY 1,2
)

SELECT
    cm.customer_id,
    cm.customer_name,
    cm.lifetime_orders,
    cm.lifetime_items,
    cm.lifetime_revenue,
    cm.first_order_date,
    cm.last_order_date,
    cm.avg_days_between_orders,
    cm.avg_order_value,
    ARRAY_AGG(
        STRUCT(
            cpp.category_id,
            cpp.category_name,
            cpp.category_purchase_count,
            cpp.total_items_in_category,
            cpp.total_spent_in_category,
            cpp.total_spent_in_category / cm.lifetime_revenue * 100 as category_revenue_percentage
        )
        ORDER BY cpp.total_spent_in_category DESC
    ) as category_preferences,
    CASE 
        WHEN cm.lifetime_orders >= 10 THEN 'VIP'
        WHEN cm.lifetime_orders >= 5 THEN 'Regular'
        WHEN cm.lifetime_orders >= 2 THEN 'Returning'
        ELSE 'New'
    END as customer_segment
FROM customer_metrics cm
LEFT JOIN customer_product_preferences cpp ON cm.customer_id = cpp.customer_id
GROUP BY 
    cm.customer_id,
    cm.customer_name,
    cm.lifetime_orders,
    cm.lifetime_items,
    cm.lifetime_revenue,
    cm.first_order_date,
    cm.last_order_date,
    cm.avg_days_between_orders,
    cm.avg_order_value