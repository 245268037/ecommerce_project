import pandas as pd

from config import path


def main():
    print(
        "开始分析订单状态口径"
    )

    df = pd.read_csv(
        path.DWD_ORDER_DETAIL,
        low_memory=False
    )

    df["payable_amount"] = pd.to_numeric(
        df["payable_amount"],
        errors="coerce"
    ).fillna(0)

    df["actual_amount"] = pd.to_numeric(
        df["actual_amount"],
        errors="coerce"
    ).fillna(0)

    # =====================
    # 1.恢复订单粒度
    # =====================

    order_df = (
        df.drop_duplicates(
            subset=["order_id"]
        )
        .copy()
    )

    # =====================
    # 2.订单状态分布
    # =====================

    status_summary = (
        order_df.groupby(
            "order_status",
            dropna=False
        )
        .agg(
            order_count=(
                "order_id",
                "nunique"
            ),
            sales_amount=(
                "payable_amount",
                "sum"
            )
        )
        .reset_index()
    )

    status_summary["order_rate"] = (
        status_summary["order_count"]
        /
        status_summary["order_count"].sum()
        *
        100
    ).round(2)

    print(
        "\n========== 订单状态分布 =========="
    )

    print(
        status_summary.to_string(
            index=False
        )
    )

    # =====================
    # 3.定义有效销售状态
    # =====================

    valid_statuses = [
        "已支付",
        "已发货",
        "已完成"
    ]

    valid_order_df = order_df[
        order_df["order_status"].isin(
            valid_statuses
        )
    ].copy()

    invalid_order_df = order_df[
        ~order_df["order_status"].isin(
            valid_statuses
        )
    ].copy()

    print(
        "\n========== 有效销售口径 =========="
    )

    print(
        f"全部订单数："
        f"{len(order_df)}"
    )

    print(
        f"有效销售订单数："
        f"{len(valid_order_df)}"
    )

    print(
        f"排除订单数："
        f"{len(invalid_order_df)}"
    )

    print(
        f"全部订单应付金额："
        f"{order_df['payable_amount'].sum():.2f}"
    )

    print(
        f"有效销售金额："
        f"{valid_order_df['payable_amount'].sum():.2f}"
    )

    print(
        f"排除订单金额："
        f"{invalid_order_df['payable_amount'].sum():.2f}"
    )

    # =====================
    # 4.检查商品明细口径
    # =====================

    valid_detail_df = df[
        df["order_status"].isin(
            valid_statuses
        )
    ].copy()

    print(
        "\n========== 有效商品销售 =========="
    )

    print(
        f"有效商品明细数："
        f"{len(valid_detail_df)}"
    )

    print(
        f"有效商品销量："
        f"{valid_detail_df['quantity'].sum()}"
    )

    print(
        f"有效商品销售额："
        f"{valid_detail_df['actual_amount'].sum():.2f}"
    )


if __name__ == "__main__":
    main()