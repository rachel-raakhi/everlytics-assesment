WITH CustomerCohort AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(order_date)) AS cohort_month
    FROM
        clean_orders 
    GROUP BY customer_id
),
MonthlyActivity AS (
    SELECT DISTINCT
        o.customer_id,
        cc.cohort_month,
        DATE_TRUNC('month', o.order_date) AS activity_month
    FROM
        clean_orders o
    JOIN
        CustomerCohort cc ON o.customer_id = cc.customer_id
),
RetentionPeriod AS (
    SELECT
        cohort_month,
        (EXTRACT(YEAR FROM activity_month) - EXTRACT(YEAR FROM cohort_month)) * 12 +
        (EXTRACT(MONTH FROM activity_month) - EXTRACT(MONTH FROM cohort_month)) AS month_number,
        COUNT(DISTINCT customer_id) AS retained_customers
    FROM
        MonthlyActivity
    GROUP BY 1, 2
)
SELECT
    t1.cohort_month,
    t1.retained_customers AS initial_cohort_size,
    t2.month_number,
    t2.retained_customers AS retained_in_month,
    ROUND((CAST(t2.retained_customers AS NUMERIC) * 100 / t1.retained_customers), 2) AS retention_rate_percent
FROM
    RetentionPeriod t1 
JOIN
    RetentionPeriod t2 ON t1.cohort_month = t2.cohort_month
WHERE
    t1.month_number = 0
ORDER BY
    t1.cohort_month, t2.month_number;