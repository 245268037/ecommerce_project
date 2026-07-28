WITH order_stats AS (
    SELECT
        COUNT(*) AS order_rows,
        COUNT(DISTINCT order_id) AS order_count
    FROM orders
),

detail_stats AS (
    SELECT
        COUNT(*) AS detail_rows,
        COUNT(DISTINCT order_id) AS detail_order_count
    FROM order_detail
),

join_stats AS (
    SELECT
        COUNT(*) AS join_rows,
        COUNT(DISTINCT o.order_id) AS join_order_count
    FROM orders o
    INNER JOIN order_detail d
        ON o.order_id = d.order_id
)

SELECT
    o.order_rows,
    o.order_count,
    d.detail_rows,
    d.detail_order_count,
    j.join_rows,
    j.join_order_count
FROM order_stats o
CROSS JOIN detail_stats d
CROSS JOIN join_stats j;
