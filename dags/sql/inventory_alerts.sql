SELECT
    p.product_id,
    p.name AS product_name,
    i.stock_on_hand,
    i.warehouse_id,
    i.last_restock_date
FROM
    products p
JOIN
    inventory i ON p.product_id = i.product_id
WHERE
    i.stock_on_hand < 50
ORDER BY
    i.stock_on_hand ASC;