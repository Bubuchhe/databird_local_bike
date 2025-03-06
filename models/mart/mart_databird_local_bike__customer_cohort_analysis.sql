WITH customer_first_purchase AS (
    SELECT
        customer_id,
        DATE_TRUNC(first_order_date, MONTH) as cohort_month
    FROM {{ref('int_sales_database__customer_order_history')}}
),

customer_monthly_activity AS (
    SELECT
        o.customer_id,
        DATE_TRUNC(o.order_date, MONTH) as activity_month,
        SUM(o.total_revenue) as revenue,
        COUNT(DISTINCT o.order_id) as number_of_orders
    FROM {{ref('int_sales_database__orders_aggregated')}} o
    GROUP BY 1, 2
),

cohort_analysis AS (
    SELECT
        cfp.cohort_month,
        cma.activity_month,
        DATE_DIFF(
            cma.activity_month,
            cfp.cohort_month,
            MONTH
        ) as months_since_first_purchase,
        COUNT(DISTINCT cfp.customer_id) as total_customers,
        SUM(cma.revenue) as total_revenue,
        SUM(cma.number_of_orders) as total_orders,
        SUM(cma.revenue) / COUNT(DISTINCT cfp.customer_id) as revenue_per_customer,
        SUM(cma.number_of_orders) / COUNT(DISTINCT cfp.customer_id) as orders_per_customer
    FROM customer_first_purchase cfp
    LEFT JOIN customer_monthly_activity cma 
        ON cfp.customer_id = cma.customer_id
    GROUP BY 1, 2, 3
)

SELECT
    cohort_month,
    activity_month,
    --months_since_first_purchase,
    (CASE 
        WHEN months_since_first_purchase = 0 THEN 0
        WHEN months_since_first_purchase BETWEEN 1 AND 6 THEN 1
        WHEN months_since_first_purchase BETWEEN 7 AND 12 THEN 2
        WHEN months_since_first_purchase BETWEEN 13 AND 18 THEN 3
        WHEN months_since_first_purchase BETWEEN 19 AND 25 THEN 4
        WHEN months_since_first_purchase > 25 THEN 5
    END) AS semester_since_first_purchase,
    total_customers,
    total_revenue,
    total_orders,
    revenue_per_customer,
    orders_per_customer,
    LAG(total_customers) OVER (
        PARTITION BY cohort_month 
        ORDER BY months_since_first_purchase
    ) as previous_month_active_customers,
    COALESCE(
        total_customers * 100.0 / NULLIF(
            FIRST_VALUE(total_customers) OVER (
                PARTITION BY cohort_month 
                ORDER BY months_since_first_purchase
            ), 
            0
        ),
        0
    ) as retention_rate
FROM cohort_analysis
WHERE months_since_first_purchase >= 0
ORDER BY cohort_month, semester_since_first_purchase