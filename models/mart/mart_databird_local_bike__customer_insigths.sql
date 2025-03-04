WITH customer_monthly_metrics AS (
    SELECT
        customer_id,
        DATE_TRUNC(order_date, MONTH) as month,
        COUNT(DISTINCT order_id) as orders_in_month,
        SUM(total_items) as items_in_month,
        SUM(total_revenue) as revenue_in_month
    FROM {{ref('int_sales_database__orders_aggregated')}}
    GROUP BY 1, 2
),

customer_trends AS (
    SELECT
        customer_id,
        month,
        orders_in_month,
        items_in_month,
        revenue_in_month,
        AVG(revenue_in_month) OVER (
            PARTITION BY customer_id 
            ORDER BY month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as moving_avg_revenue,
        LAG(revenue_in_month) OVER (
            PARTITION BY customer_id 
            ORDER BY month
        ) as previous_month_revenue
    FROM customer_monthly_metrics
)

SELECT
    ch.customer_id,
    ch.customer_name,
    ch.lifetime_orders,
    ch.lifetime_items,
    ch.lifetime_revenue,
    ch.first_order_date,
    ch.last_order_date,
    ch.avg_days_between_orders,
    ch.avg_order_value,
    ch.customer_segment,
    ch.category_preferences,
    ct.month as last_active_month,
    ct.orders_in_month as last_month_orders,
    ct.revenue_in_month as last_month_revenue,
    DATE_DIFF(CURRENT_DATE(), ch.last_order_date, DAY) as days_since_last_order,
    CASE 
        WHEN DATE_DIFF(CURRENT_DATE(), ch.last_order_date, DAY) <= 30 THEN 'Active'
        WHEN DATE_DIFF(CURRENT_DATE(), ch.last_order_date, DAY) <= 90 THEN 'At Risk'
        ELSE 'Churned'
    END as activity_status
FROM {{ref('int_sales_database__customer_order_history')}} ch
LEFT JOIN customer_trends ct 
    ON ch.customer_id = ct.customer_id
    AND ct.month = DATE_TRUNC(ch.last_order_date, MONTH)