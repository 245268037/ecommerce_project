SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_detail_id) AS distinct_detail_count,
    COUNT(DISTINCT order_id) AS distinct_order_count,
    COUNT(*) - COUNT(DISTINCT order_detail_id) AS duplicate_detail_count,
    ROUND(
        COUNT(*) * 1.0
        / NULLIF(
            COUNT(DISTINCT order_id),
            0
        ),
        2
    ) AS avg_details_per_order
FROM order_detail;
