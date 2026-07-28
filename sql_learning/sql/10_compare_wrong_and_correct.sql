WITH valid_orders AS (
    SELECT
        order_id,
        order_channel,
        payable_amount
    FROM orders
    WHERE order_status IN (
        '已支付',
        '已发货',
        '已完成'
    )
),

wrong_metrics AS (
    SELECT
        o.order_channel,
        COUNT(DISTINCT o.order_id) AS order_count,
        SUM(o.payable_amount) AS wrong_sales_amount
    FROM valid_orders o
    JOIN order_detail d
        ON o.order_id = d.order_id
    GROUP BY o.order_channel
),

correct_metrics AS (
    SELECT
        order_channel,
        COUNT(*) AS order_count,
        SUM(payable_amount) AS correct_sales_amount
    FROM valid_orders
    GROUP BY order_channel
)

SELECT
    c.order_channel,
    c.order_count,
    ROUND(
        c.correct_sales_amount,
        2
    ) AS correct_sales_amount,
    ROUND(
        w.wrong_sales_amount,
        2
    ) AS wrong_sales_amount,
    ROUND(
        w.wrong_sales_amount
        - c.correct_sales_amount,
        2
    ) AS repeated_amount,
    ROUND(
        w.wrong_sales_amount
        / NULLIF(
            c.correct_sales_amount,
            0
        ),
        4
    ) AS amplification_ratio
FROM correct_metrics c
JOIN wrong_metrics w
    ON c.order_channel = w.order_channel
ORDER BY
    amplification_ratio DESC;
