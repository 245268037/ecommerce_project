SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_order_count,
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_count
FROM orders;
