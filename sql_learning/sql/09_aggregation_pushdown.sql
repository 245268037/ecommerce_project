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

order_metrics AS (
    SELECT
        order_channel,
        COUNT(*) AS order_count,
        SUM(payable_amount) AS sales_amount
    FROM valid_orders
    GROUP BY order_channel
),

detail_metrics AS (
    SELECT
        o.order_channel,
        SUM(d.quantity) AS sales_quantity,
        SUM(d.actual_amount) AS detail_amount
    FROM valid_orders o
    JOIN order_detail d
        ON o.order_id = d.order_id
    GROUP BY o.order_channel
)

SELECT
    o.order_channel,
    o.order_count,
    ROUND(
        o.sales_amount,
        2
    ) AS sales_amount,
    d.sales_quantity,
    ROUND(
        d.detail_amount,
        2
    ) AS detail_amount,

    ROUND(
        o.sales_amount
        / NULLIF(
            o.order_count,
            0
        ),
        2
    ) AS avg_order_amount

FROM order_metrics o
LEFT JOIN detail_metrics d
    ON o.order_channel = d.order_channel

ORDER BY
    o.sales_amount DESC;
