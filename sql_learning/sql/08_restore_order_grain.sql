WITH order_level AS (
    SELECT
        o.order_id,
        MAX(o.payable_amount) AS payable_amount,
        COUNT(*) AS detail_count
    FROM orders o
    INNER JOIN order_detail d
        ON o.order_id = d.order_id
    GROUP BY
        o.order_id
)

SELECT
    COUNT(*) AS order_count,
    SUM(payable_amount) AS restored_order_amount,
    SUM(detail_count) AS detail_count
FROM order_level;
