SELECT
    o.order_id,
    o.payable_amount,
    COUNT(*) AS detail_count,
    SUM(d.quantity) AS total_quantity,
    SUM(d.actual_amount) AS detail_amount
FROM orders o
INNER JOIN order_detail d
    ON o.order_id = d.order_id
GROUP BY
    o.order_id,
    o.payable_amount
ORDER BY
    detail_count DESC,
    o.order_id
LIMIT 10;
