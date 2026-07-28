WITH order_amount AS (
    SELECT
        SUM(payable_amount) AS correct_order_amount
    FROM orders
),

joined_amount AS (
    SELECT
        SUM(o.payable_amount) AS wrong_joined_amount
    FROM orders o
    INNER JOIN order_detail d
        ON o.order_id = d.order_id
)

SELECT
    o.correct_order_amount,
    j.wrong_joined_amount,

    j.wrong_joined_amount
        - o.correct_order_amount
        AS repeated_amount,

    ROUND(
        j.wrong_joined_amount
        / NULLIF(
            o.correct_order_amount,
            0
        ),
        4
    ) AS amplification_ratio
FROM order_amount o
CROSS JOIN joined_amount j;
