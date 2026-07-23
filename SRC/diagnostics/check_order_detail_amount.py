from pathlib import Path

import pandas as pd

from config import path


def main():
    print(
        "开始执行订单与明细金额核对"
    )

    # =====================
    # 1.读取DWD
    # =====================

    df = pd.read_csv(
        path.DWD_ORDER_DETAIL,
        low_memory=False
    )

    required_columns = [
        "order_id",
        "order_amount",
        "coupon_amount",
        "freight_amount",
        "payable_amount",
        "actual_amount"
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"DWD缺少必要字段："
            f"{missing_columns}"
        )

    # =====================
    # 2.统一数值类型
    # =====================

    amount_columns = [
        "order_amount",
        "coupon_amount",
        "freight_amount",
        "payable_amount",
        "actual_amount"
    ]

    for column in amount_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        ).fillna(0)

    # =====================
    # 3.提取订单头
    # =====================

    order_header = (
        df[
            [
                "order_id",
                "order_amount",
                "coupon_amount",
                "freight_amount",
                "payable_amount"
            ]
        ]
        .drop_duplicates(
            subset=["order_id"]
        )
        .copy()
    )

    # =====================
    # 4.汇总明细金额
    # =====================

    detail_summary = (
        df.groupby(
            "order_id",
            as_index=False
        )
        .agg(
            detail_amount=(
                "actual_amount",
                "sum"
            ),
            detail_count=(
                "order_id",
                "size"
            )
        )
    )

    # =====================
    # 5.订单与明细关联
    # =====================

    check_df = order_header.merge(
        detail_summary,
        on="order_id",
        how="left",
        validate="one_to_one"
    )

    # =====================
    # 6.检查订单金额
    # =====================

    check_df["order_amount_difference"] = (
        check_df["detail_amount"]
        -
        check_df["order_amount"]
    ).abs()

    check_df["order_amount_match"] = (
        check_df["order_amount_difference"]
        <= 0.01
    )

    # =====================
    # 7.检查应付金额公式
    # =====================

    check_df["calculated_payable_amount"] = (
        check_df["order_amount"]
        -
        check_df["coupon_amount"]
        +
        check_df["freight_amount"]
    ).round(2)

    check_df["payable_difference"] = (
        check_df["calculated_payable_amount"]
        -
        check_df["payable_amount"]
    ).abs()

    check_df["payable_match"] = (
        check_df["payable_difference"]
        <= 0.01
    )

    # =====================
    # 8.汇总核对结果
    # =====================

    total_order_count = len(check_df)

    order_amount_match_count = (
        check_df["order_amount_match"]
        .sum()
    )

    payable_match_count = (
        check_df["payable_match"]
        .sum()
    )

    detail_total = (
        check_df["detail_amount"]
        .sum()
    )

    order_total = (
        check_df["order_amount"]
        .sum()
    )

    coupon_total = (
        check_df["coupon_amount"]
        .sum()
    )

    freight_total = (
        check_df["freight_amount"]
        .sum()
    )

    payable_total = (
        check_df["payable_amount"]
        .sum()
    )

    print(
        f"订单总数：{total_order_count}"
    )

    print(
        f"订单金额与明细一致数："
        f"{order_amount_match_count}"
    )

    print(
        f"应付金额公式一致数："
        f"{payable_match_count}"
    )

    print(
        f"商品明细金额合计："
        f"{detail_total:.2f}"
    )

    print(
        f"订单金额合计："
        f"{order_total:.2f}"
    )

    print(
        f"优惠券合计："
        f"{coupon_total:.2f}"
    )

    print(
        f"运费合计："
        f"{freight_total:.2f}"
    )

    print(
        f"应付金额合计："
        f"{payable_total:.2f}"
    )

    # =====================
    # 9.提取异常订单
    # =====================

    error_df = check_df[
        ~check_df["order_amount_match"]
        |
        ~check_df["payable_match"]
    ].copy()

    project_root = (
        Path(__file__)
        .resolve()
        .parent
        .parent
    )

    output_dir = (
        project_root
        / "output"
        / "quality"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        output_dir
        / "order_amount_check_error.csv"
    )

    error_df.to_csv(
        output_file,
        index=False,
        encoding="utf-8-sig"
    )

    print(
        f"金额异常订单数："
        f"{len(error_df)}"
    )

    print(
        f"异常文件：{output_file}"
    )

    # =====================
    # 10.最终判断
    # =====================

    if len(error_df) > 0:
        raise ValueError(
            f"订单金额核对失败，"
            f"异常订单数：{len(error_df)}"
        )

    print(
        "全部订单金额核对通过"
    )


if __name__ == "__main__":
    main()