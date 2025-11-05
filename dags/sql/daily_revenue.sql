-- This query uses SELECT INTO to store the result in a table needed by the reporting Python task.
SELECT
    t1.order_date,
    SUM(t1.daily_revenue) AS total_daily_revenue,
    (SELECT category FROM RankedCategories WHERE order_date = t1.order_date AND category_rank = 1 LIMIT 1) AS top_category
INTO daily_revenue_report
FROM
    (
        WITH DailySales AS (
            SELECT
                order_date,
                category,
                SUM(CASE WHEN is_returned = FALSE THEN order_total ELSE 0 END) AS daily_revenue 
            FROM clean_orders 
            WHERE 
                order_date = '{{ ds }}' 
            GROUP BY order_date, category
        ),
        RankedCategories AS (
            SELECT
                *,
                RANK() OVER (PARTITION BY order_date ORDER BY daily_revenue DESC) AS category_rank
            FROM DailySales
        )
        SELECT order_date, daily_revenue, category
        FROM DailySales
    ) t1
GROUP BY
    t1.order_date;