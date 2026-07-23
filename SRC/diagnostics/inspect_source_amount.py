from pathlib import Path

import pandas as pd


def main():
    project_root = (
        Path(__file__)
        .resolve()
        .parent
        .parent
    )

    order_file = (
        project_root
        / "data"
        / "clean"
        / "cleaner_order.csv"
    )

    detail_file = (
        project_root
        / "data"
        / "clean"
        / "cleaner_order_detail.csv"
    )

    order_df = pd.read_csv(
        order_file,
        low_memory=False
    )

    detail_df = pd.read_csv(
        detail_file,
        low_memory=False
    )

    print(
        "========== 订单表字段 =========="
    )
    print(
        order_df.columns.tolist()
    )

    print(
        "\n========== 明细表字段 =========="
    )
    print(
        detail_df.columns.tolist()
    )

    # =====================
    # 1.数值类型转换
    # =====================

    order_numeric_columns = [
        "order_amount",
        "coupon_amount",
        "freight_amount",
        "payable_amount"
    ]

    detail_numeric_columns = [
        "quantity",
        "actual_amount"
    ]

    for column in order_numeric_columns:
        if column in order_df.columns:
            order_df[column] = pd.to_numeric(
                order_df[column],
                errors="coerce"
            )

    for column in detail_numeric_columns:
        if column in detail_df.columns:
            detail_df[column] = pd.to_numeric(
                detail_df[column],
                errors="coerce"
            )

    # =====================
    # 2.检查商品数量
    # =====================

    print(
        "\n========== quantity统计 =========="
    )

    print(
        detail_df["quantity"]
        .describe(
            percentiles=[
                0.25,
                0.5,
                0.75,
                0.9,
                0.99
            ]
        )
    )

    print(
        "\nquantity大于10的明细数：",
        (
            detail_df["quantity"] > 10
        ).sum()
    )

    print(
        "quantity小于等于0的明细数：",
        (
            detail_df["quantity"] <= 0
        ).sum()
    )

    # =====================
    # 3.检查明细金额
    # =====================

    print(
        "\n========== actual_amount统计 =========="
    )

    print(
        detail_df["actual_amount"]
        .describe(
            percentiles=[
                0.25,
                0.5,
                0.75,
                0.9,
                0.99
            ]
        )
    )

    print(
        "\nactual_amount小于0的明细数：",
        (
            detail_df["actual_amount"] < 0
        ).sum()
    )

    # =====================
    # 4.检查订单金额
    # =====================

    print(
        "\n========== payable_amount统计 =========="
    )

    print(
        order_df["payable_amount"]
        .describe(
            percentiles=[
                0.25,
                0.5,
                0.75,
                0.9,
                0.99
            ]
        )
    )

    print(
        "\npayable_amount等于0的订单数：",
        (
            order_df["payable_amount"] == 0
        ).sum()
    )

    print(
        "payable_amount小于0的订单数：",
        (
            order_df["payable_amount"] < 0
        ).sum()
    )

    # =====================
    # 5.检查订单金额公式
    # =====================

    formula_columns = [
        "order_amount",
        "coupon_amount",
        "freight_amount",
        "payable_amount"
    ]

    if all(
        column in order_df.columns
        for column in formula_columns
    ):
        calculated_payable = (
            order_df["order_amount"]
            -
            order_df["coupon_amount"]
            +
            order_df["freight_amount"]
        )

        formula_difference = (
            calculated_payable
            -
            order_df["payable_amount"]
        ).abs()

        print(
            "\n订单应付金额公式异常数：",
            (
                formula_difference > 0.01
            ).sum()
        )

    # =====================
    # 6.查看异常订单
    # =====================

    sample_order_ids = [
        "O00076658",
        "O00062083",
        "O00099010"
    ]

    print(
        "\n========== 异常订单头 =========="
    )

    print(
        order_df[
            order_df["order_id"].isin(
                sample_order_ids
            )
        ].to_string(
            index=False
        )
    )

    print(
        "\n========== 异常订单明细 =========="
    )

    print(
        detail_df[
            detail_df["order_id"].isin(
                sample_order_ids
            )
        ].to_string(
            index=False
        )
    )


if __name__ == "__main__":
    main()