SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT customer_id) AS distinct_customer_count,
    COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_count
FROM customer;
