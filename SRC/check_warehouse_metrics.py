import pandas as pd

from config import path
from config.settings import VALID_SALES_STATUSES


def read_csv(file_path):
    """统一读取CSV文件。"""

    return pd.read_csv(
        file_path,
        low_memory=False
    )


def convert_numeric(
        df,
        columns
):
    """将指定字段统一转换为数值类型。"""

    for column in columns:
        if column not in df.columns:
            raise ValueError(
                f"数据缺少数值字段：{column}"
            )

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        ).fillna(0)

    return df


def compare(
        check_name,
        source_value,
        target_value,
        tolerance=0
):
    """比较上下游指标是否一致。"""

    difference = abs(
        source_value
        -
        target_value
    )

    passed = (
        difference
        <= tolerance
    )

    return {
        "check_name": check_name,
        "source_value": source_value,
        "target_value": target_value,
        "difference": difference,
        "status": (
            "通过"
            if passed
            else "失败"
        )
    }


def main():
    print(
        "开始执行数仓跨层指标核对"
    )

    # =====================
    # 1.读取各层数据
    # =====================

    dwd = read_csv(
        path.DWD_ORDER_DETAIL
    )

    dws_user = read_csv(
        path.DWS_USER_SALES
    )

    dws_product = read_csv(
        path.DWS_PRODUCT_SALES
    )

    ads_sales = read_csv(
        path.ADS_SALES_SUMMARY
    )

    ads_user = read_csv(
        path.ADS_USER_SUMMARY
    )

    ads_product = read_csv(
        path.ADS_PRODUCT_SUMMARY
    )

    # =====================
    # 2.检查必要字段
    # =====================

    required_columns = {
        "DWD": [
            "order_id",
            "order_status",
            "order_time",
            "quantity",
            "actual_amount",
            "payable_amount"
        ],
        "DWS用户主题": [
            "order_count",
            "total_amount"
        ],
        "DWS商品主题": [
            "sales_count",
            "sales_amount"
        ],
        "ADS日销售": [
            "order_count",
            "sales_amount"
        ],
        "ADS用户指标": [
            "order_count",
            "total_amount"
        ],
        "ADS商品指标": [
            "sales_count",
            "sales_amount"
        ]
    }

    dataframes = {
        "DWD": dwd,
        "DWS用户主题": dws_user,
        "DWS商品主题": dws_product,
        "ADS日销售": ads_sales,
        "ADS用户指标": ads_user,
        "ADS商品指标": ads_product
    }

    for df_name, columns in required_columns.items():
        missing_columns = [
            column
            for column in columns
            if column not in dataframes[df_name].columns
        ]

        if missing_columns:
            raise ValueError(
                f"{df_name}缺少必要字段："
                f"{missing_columns}"
            )

    # =====================
    # 3.统一数值类型
    # =====================

    dwd = convert_numeric(
        dwd,
        [
            "quantity",
            "actual_amount",
            "payable_amount"
        ]
    )

    dws_user = convert_numeric(
        dws_user,
        [
            "order_count",
            "total_amount"
        ]
    )

    dws_product = convert_numeric(
        dws_product,
        [
            "sales_count",
            "sales_amount"
        ]
    )

    ads_sales = convert_numeric(
        ads_sales,
        [
            "order_count",
            "sales_amount"
        ]
    )

    ads_user = convert_numeric(
        ads_user,
        [
            "order_count",
            "total_amount"
        ]
    )

    ads_product = convert_numeric(
        ads_product,
        [
            "sales_count",
            "sales_amount"
        ]
    )

    # =====================
    # 4.过滤有效销售数据
    # =====================

    valid_dwd = (
        dwd[
            dwd["order_status"].isin(
                VALID_SALES_STATUSES
            )
        ]
        .copy()
    )

    # DWD是一行一条商品明细。
    # 用户和日销售指标需要恢复到订单粒度。
    valid_dwd_order = (
        valid_dwd.drop_duplicates(
            subset=["order_id"]
        )
        .copy()
    )

    valid_dwd_order["order_time"] = (
        pd.to_datetime(
            valid_dwd_order["order_time"],
            errors="coerce"
        )
    )

    # ADS日销售无法统计订单时间无效的订单。
    valid_time_order = (
        valid_dwd_order[
            valid_dwd_order[
                "order_time"
            ].notna()
        ]
        .copy()
    )

    # =====================
    # 5.输出核对口径
    # =====================

    all_order_count = (
        dwd["order_id"]
        .nunique()
    )

    valid_order_count = (
        valid_dwd_order["order_id"]
        .nunique()
    )

    excluded_order_count = (
        all_order_count
        -
        valid_order_count
    )

    invalid_time_count = (
        valid_dwd_order["order_time"]
        .isna()
        .sum()
    )

    print(
        "\n========== 跨层核对口径 =========="
    )

    print(
        f"全部订单数："
        f"{all_order_count}"
    )

    print(
        f"有效销售订单数："
        f"{valid_order_count}"
    )

    print(
        f"排除订单数："
        f"{excluded_order_count}"
    )

    print(
        f"有效商品明细数："
        f"{len(valid_dwd)}"
    )

    print(
        f"订单时间无效数："
        f"{invalid_time_count}"
    )

    print(
        f"有效销售状态："
        f"{VALID_SALES_STATUSES}"
    )

    # =====================
    # 6.执行跨层核对
    # =====================

    results = []

    # ---------------------
    # 用户主题核对
    # ---------------------

    results.append(
        compare(
            "DWD有效订单数 → DWS用户订单数",
            valid_dwd_order[
                "order_id"
            ].nunique(),
            dws_user[
                "order_count"
            ].sum()
        )
    )

    results.append(
        compare(
            "DWD有效订单金额 → DWS用户金额",
            valid_dwd_order[
                "payable_amount"
            ].sum(),
            dws_user[
                "total_amount"
            ].sum(),
            tolerance=0.01
        )
    )

    # ---------------------
    # ADS日销售核对
    # ---------------------

    results.append(
        compare(
            "DWD有效日期订单数 → ADS日销售订单数",
            valid_time_order[
                "order_id"
            ].nunique(),
            ads_sales[
                "order_count"
            ].sum()
        )
    )

    results.append(
        compare(
            "DWD有效日期订单金额 → ADS日销售金额",
            valid_time_order[
                "payable_amount"
            ].sum(),
            ads_sales[
                "sales_amount"
            ].sum(),
            tolerance=0.01
        )
    )

    # ---------------------
    # DWS用户与ADS用户核对
    # ---------------------

    results.append(
        compare(
            "DWS用户数 → ADS用户数",
            len(dws_user),
            len(ads_user)
        )
    )

    results.append(
        compare(
            "DWS用户订单数 → ADS用户订单数",
            dws_user[
                "order_count"
            ].sum(),
            ads_user[
                "order_count"
            ].sum()
        )
    )

    results.append(
        compare(
            "DWS用户金额 → ADS用户金额",
            dws_user[
                "total_amount"
            ].sum(),
            ads_user[
                "total_amount"
            ].sum(),
            tolerance=0.01
        )
    )

    # ---------------------
    # 商品主题核对
    # ---------------------

    results.append(
        compare(
            "DWD有效商品销量 → DWS商品销量",
            valid_dwd[
                "quantity"
            ].sum(),
            dws_product[
                "sales_count"
            ].sum()
        )
    )

    results.append(
        compare(
            "DWD有效商品金额 → DWS商品金额",
            valid_dwd[
                "actual_amount"
            ].sum(),
            dws_product[
                "sales_amount"
            ].sum(),
            tolerance=0.01
        )
    )

    # ---------------------
    # DWS商品与ADS商品核对
    # ---------------------

    results.append(
        compare(
            "DWS商品数 → ADS商品数",
            len(dws_product),
            len(ads_product)
        )
    )

    results.append(
        compare(
            "DWS商品销量 → ADS商品销量",
            dws_product[
                "sales_count"
            ].sum(),
            ads_product[
                "sales_count"
            ].sum()
        )
    )

    results.append(
        compare(
            "DWS商品金额 → ADS商品金额",
            dws_product[
                "sales_amount"
            ].sum(),
            ads_product[
                "sales_amount"
            ].sum(),
            tolerance=0.01
        )
    )

    # =====================
    # 7.输出核对报告
    # =====================

    report = pd.DataFrame(
        results
    )

    print(
        "\n========== 数仓指标核对报告 =========="
    )

    print(
        report.to_string(
            index=False
        )
    )

    # =====================
    # 8.处理失败结果
    # =====================

    failed_report = report[
        report["status"] == "失败"
    ]

    if not failed_report.empty:
        print(
            "\n========== 核对失败项目 =========="
        )

        print(
            failed_report.to_string(
                index=False
            )
        )

        raise ValueError(
            "数仓跨层指标核对失败，"
            f"失败项数量："
            f"{len(failed_report)}"
        )

    print(
        "\n全部跨层指标核对通过"
    )


if __name__ == "__main__":
    main()