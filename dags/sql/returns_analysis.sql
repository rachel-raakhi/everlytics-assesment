SELECT
    o.product_id,
    p.name AS product_name,
    COUNT(o.order_id) AS total_orders,
    COUNT(CASE WHEN o.is_returned = TRUE THEN 1 END) AS total_returns,
    ROUND(
        (COUNT(CASE WHEN o.is_returned = TRUE THEN 1 END)::NUMERIC / NULLIF(COUNT(o.order_id), 0)) * 100, 
        2
    ) AS return_rate_percent
FROM
    clean_orders o
JOIN
    products p ON o.product_id = p.product_id
GROUP BY 
    o.product_id, p.name
ORDER BY 
    return_rate_percent DESC;