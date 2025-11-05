SELECT
    o.product_id,
    p.name AS product_name,
    p.category,
    SUM(o.quantity) AS total_units_sold,
    SUM(CASE WHEN o.is_returned = FALSE THEN o.order_total ELSE 0 END) AS net_revenue,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(
        CAST(SUM(CASE WHEN o.is_returned = TRUE THEN o.quantity ELSE 0 END) AS NUMERIC) 
        / NULLIF(SUM(o.quantity), 0) * 100, 
        2
    ) AS return_rate_percent
FROM
    clean_orders o
JOIN
    products p ON o.product_id = p.product_id
GROUP BY
    o.product_id, p.name, p.category
ORDER BY
    net_revenue DESC
LIMIT 10;