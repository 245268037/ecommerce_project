"""
Spark数仓最终指标验收。

主要检查：
1. ODS与DWD数据量是否一致。
2. DWD与DWS订单、用户、商品指标是否一致。
3. DWS与ADS指标是否一致。
4. RFM用户和金额是否完整。
5. 商品ABC排名、累计占比是否正确。

本程序只读取数据，不会修改数仓文件。
"""

from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. 项目路径
# =========================================================

CURRENT_FILE = Path(__file__).resolve()
PROJECT_DIR = CURRENT_FILE.parent.parent
WAREHOUSE_DIR = PROJECT_DIR / "warehouse_spark"

PATHS = {
    "ods_order": (
        WAREHOUSE_DIR / "ods" / "ods_order"
    ),
    "ods_order_detail": (
        WAREHOUSE_DIR / "ods" / "ods_order_detail"
    ),
    "ods_customer": (
        WAREHOUSE_DIR / "ods" / "ods_customer"
    ),
    "ods_product": (
        WAREHOUSE_DIR / "ods" / "ods_product"
    ),
    "dwd_order_detail": (
        WAREHOUSE_DIR / "dwd" / "dwd_order_detail"
    ),
    "dws_user_sales": (
        WAREHOUSE_DIR / "dws" / "dws_user_sales"
    ),
    "dws_product_sales": (
        WAREHOUSE_DIR / "dws" / "dws_product_sales"
    ),
    "dws_area_sales": (
        WAREHOUSE_DIR / "dws" / "dws_area_sales"
    ),
    "ads_sales_summary": (
        WAREHOUSE_DIR / "ads" / "ads_sales_summary"
    ),
    "ads_monthly_sales_trend": (
        WAREHOUSE_DIR
        / "ads"
        / "ads_monthly_sales_trend"
    ),
    "ads_user_rfm_base": (
        WAREHOUSE_DIR / "ads" / "ads_user_rfm_base"
    ),
    "ads_user_rfm_segment": (
        WAREHOUSE_DIR
        / "ads"
        / "ads_user_rfm_segment"
    ),
    "ads_product_summary": (
        WAREHOUSE_DIR / "ads" / "ads_product_summary"
    ),
    "ads_product_abc_summary": (
        WAREHOUSE_DIR
        / "ads"
        / "ads_product_abc_summary"
    ),
}


# =========================================================
# 2. 有效销售状态
# =========================================================

VALID_SALES_STATUSES = [
    "已支付",
    "已发货",
    "已完成",
]


# =========================================================
# 3. 检查结果
# =========================================================

CHECK_RESULTS = []


# =========================================================
# 4. 数值转换函数
# =========================================================

def to_decimal(value):
    """
    把整数、小数和空值统一转换成Decimal。
    """

    if value is None:
        return Decimal("0")

    return Decimal(
        str(value)
    )


# =========================================================
# 5. 记录比较结果
# =========================================================

def compare(
        check_name,
        source_value,
        target_value,
        tolerance=0
):
    """
    比较源指标和目标指标。
    """

    source_number = to_decimal(
        source_value
    )

    target_number = to_decimal(
        target_value
    )

    tolerance_number = to_decimal(
        tolerance
    )

    difference = abs(
        source_number
        - target_number
    )

    passed = (
        difference
        <= tolerance_number
    )

    CHECK_RESULTS.append({
        "check_name": check_name,
        "source_value": source_number,
        "target_value": target_number,
        "difference": difference,
        "status": (
            "通过"
            if passed
            else "失败"
        ),
    })


# =========================================================
# 6. 记录自定义检查结果
# =========================================================

def record_check(
        check_name,
        source_value,
        target_value,
        passed
):
    """
    记录无法直接做减法的检查。
    """

    CHECK_RESULTS.append({
        "check_name": check_name,
        "source_value": source_value,
        "target_value": target_value,
        "difference": "-",
        "status": (
            "通过"
            if passed
            else "失败"
        ),
    })


# =========================================================
# 7. 查找字段
# =========================================================

def find_column(
        dataframe,
        table_name,
        candidate_columns
):
    """
    从多个候选字段名中找到实际存在的字段。
    """

    for column_name in candidate_columns:
        if column_name in dataframe.columns:
            return column_name

    raise ValueError(
        f"{table_name}找不到指标字段，"
        f"候选字段={candidate_columns}，"
        f"实际字段={dataframe.columns}"
    )


# =========================================================
# 8. 检查必要字段
# =========================================================

def check_required_columns(
        dataframe,
        table_name,
        required_columns
):
    """
    检查数据表是否缺少必要字段。
    """

    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{table_name}缺少必要字段："
            f"{missing_columns}"
        )


# =========================================================
# 9. 汇总某个字段
# =========================================================

def sum_column(
        dataframe,
        column_name
):
    """
    计算某一列的合计值。
    """

    result = (
        dataframe
        .agg(
            F.sum(
                F.col(column_name)
            ).alias("total_value")
        )
        .first()
    )

    return (
        result["total_value"]
        or 0
    )


# =========================================================
# 10. 读取Parquet
# =========================================================

def read_parquet(
        spark,
        table_name
):
    """
    读取指定数仓表。
    """

    table_path = PATHS[
        table_name
    ]

    if not table_path.exists():
        raise FileNotFoundError(
            f"数仓目录不存在：{table_path}"
        )

    dataframe = (
        spark.read.parquet(
            str(table_path)
        )
    )

    print(
        f"读取完成：{table_name}"
    )

    return dataframe


# =========================================================
# 11. 打印检查报告
# =========================================================

def print_report():
    """
    打印最终验收报告。
    """

    print(
        "\n========== Spark数仓最终验收报告 =========="
    )

    for index, result in enumerate(
            CHECK_RESULTS,
            start=1
    ):
        print(
            f"\n{index}. {result['check_name']}"
        )

        print(
            f"   源指标：{result['source_value']}"
        )

        print(
            f"   目标指标：{result['target_value']}"
        )

        print(
            f"   差异：{result['difference']}"
        )

        print(
            f"   结果：{result['status']}"
        )

    failed_results = [
        result
        for result in CHECK_RESULTS
        if result["status"] == "失败"
    ]

    print(
        "\n==========================================="
    )

    print(
        f"检查项总数：{len(CHECK_RESULTS)}"
    )

    print(
        f"通过项数量："
        f"{len(CHECK_RESULTS) - len(failed_results)}"
    )

    print(
        f"失败项数量：{len(failed_results)}"
    )

    if failed_results:
        print(
            "\nSpark数仓最终验收失败"
        )

        print(
            "失败项目："
        )

        for result in failed_results:
            print(
                f"- {result['check_name']}"
            )

        raise ValueError(
            "Spark数仓存在跨层指标不一致"
        )

    print(
        "\n全部跨层指标核对通过"
    )

    print(
        "Spark数仓最终验收成功"
    )


# =========================================================
# 12. 主程序
# =========================================================

def main():
    spark = (
        SparkSession.builder
        .appName(
            "CheckSparkWarehouseMetrics"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:
        print(
            "\n========== 开始Spark数仓最终验收 =========="
        )

        # =================================================
        # 13. 读取数据
        # =================================================

        ods_order = read_parquet(
            spark,
            "ods_order"
        )

        ods_order_detail = read_parquet(
            spark,
            "ods_order_detail"
        )

        ods_customer = read_parquet(
            spark,
            "ods_customer"
        )

        ods_product = read_parquet(
            spark,
            "ods_product"
        )

        dwd = read_parquet(
            spark,
            "dwd_order_detail"
        )

        dws_user = read_parquet(
            spark,
            "dws_user_sales"
        )

        dws_product = read_parquet(
            spark,
            "dws_product_sales"
        )

        dws_area = read_parquet(
            spark,
            "dws_area_sales"
        )

        ads_sales = read_parquet(
            spark,
            "ads_sales_summary"
        )

        ads_monthly = read_parquet(
            spark,
            "ads_monthly_sales_trend"
        )

        rfm_base = read_parquet(
            spark,
            "ads_user_rfm_base"
        )

        rfm_segment = read_parquet(
            spark,
            "ads_user_rfm_segment"
        )

        ads_product = read_parquet(
            spark,
            "ads_product_summary"
        )

        abc_summary = read_parquet(
            spark,
            "ads_product_abc_summary"
        )

        # =================================================
        # 14. 检查DWD必要字段
        # =================================================

        check_required_columns(
            dwd,
            "DWD订单明细",
            [
                "order_id",
                "customer_id",
                "product_id",
                "order_status",
                "order_time",
                "payable_amount",
                "quantity",
                "actual_amount",
            ]
        )

        # 缓存经常使用的数据，避免重复读取
        dwd.cache()

        # =================================================
        # 15. ODS与DWD数据量检查
        # =================================================

        ods_order_count = (
            ods_order.count()
        )

        ods_detail_count = (
            ods_order_detail.count()
        )

        ods_customer_count = (
            ods_customer.count()
        )

        ods_product_count = (
            ods_product.count()
        )

        dwd_detail_count = (
            dwd.count()
        )

        dwd_order_count = (
            dwd
            .select("order_id")
            .distinct()
            .count()
        )

        compare(
            "ODS订单数 → DWD订单数",
            ods_order_count,
            dwd_order_count
        )

        compare(
            "ODS订单明细数 → DWD明细数",
            ods_detail_count,
            dwd_detail_count
        )

        # =================================================
        # 16. 过滤有效销售数据
        # =================================================

        valid_detail = (
            dwd
            .filter(
                F.col("order_status").isin(
                    VALID_SALES_STATUSES
                )
            )
            .cache()
        )

        # 从商品明细恢复成一行一个订单
        valid_order = (
            valid_detail
            .select(
                "order_id",
                "customer_id",
                "order_time",
                "payable_amount",
            )
            .dropDuplicates(
                ["order_id"]
            )
            .cache()
        )

        valid_order_count = (
            valid_order.count()
        )

        valid_customer_count = (
            valid_order
            .select("customer_id")
            .filter(
                F.col("customer_id").isNotNull()
            )
            .distinct()
            .count()
        )

        valid_product_count = (
            valid_detail
            .select("product_id")
            .filter(
                F.col("product_id").isNotNull()
            )
            .distinct()
            .count()
        )

        valid_date_count = (
            valid_order
            .select(
                F.to_date(
                    "order_time"
                ).alias("order_date")
            )
            .filter(
                F.col("order_date").isNotNull()
            )
            .distinct()
            .count()
        )

        valid_order_amount = sum_column(
            valid_order,
            "payable_amount"
        )

        valid_product_quantity = sum_column(
            valid_detail,
            "quantity"
        )

        valid_product_amount = sum_column(
            valid_detail,
            "actual_amount"
        )

        print(
            "\n========== DWD有效销售口径 =========="
        )

        print(
            f"有效订单数：{valid_order_count}"
        )

        print(
            f"有效客户数：{valid_customer_count}"
        )

        print(
            f"有效商品数：{valid_product_count}"
        )

        print(
            f"有效日期数：{valid_date_count}"
        )

        print(
            f"订单销售额：{valid_order_amount}"
        )

        print(
            f"商品销量：{valid_product_quantity}"
        )

        print(
            f"商品销售额：{valid_product_amount}"
        )

        # =================================================
        # 17. 找到各层指标字段
        # =================================================

        dws_user_order_column = find_column(
            dws_user,
            "DWS用户主题",
            ["order_count"]
        )

        dws_user_amount_column = find_column(
            dws_user,
            "DWS用户主题",
            ["total_amount", "sales_amount"]
        )

        dws_product_quantity_column = find_column(
            dws_product,
            "DWS商品主题",
            [
                "sales_count",
                "sales_quantity",
                "total_quantity",
            ]
        )

        dws_product_amount_column = find_column(
            dws_product,
            "DWS商品主题",
            [
                "sales_amount",
                "total_amount",
            ]
        )

        dws_area_order_column = find_column(
            dws_area,
            "DWS地区主题",
            ["order_count"]
        )

        dws_area_amount_column = find_column(
            dws_area,
            "DWS地区主题",
            ["sales_amount", "total_amount"]
        )

        ads_order_column = find_column(
            ads_sales,
            "ADS日销售指标",
            ["order_count"]
        )

        ads_amount_column = find_column(
            ads_sales,
            "ADS日销售指标",
            ["sales_amount", "total_amount"]
        )

        monthly_order_column = find_column(
            ads_monthly,
            "ADS月度趋势",
            ["order_count"]
        )

        monthly_amount_column = find_column(
            ads_monthly,
            "ADS月度趋势",
            ["sales_amount", "total_amount"]
        )

        rfm_base_amount_column = find_column(
            rfm_base,
            "RFM基础指标",
            ["monetary", "total_amount"]
        )

        rfm_segment_amount_column = find_column(
            rfm_segment,
            "RFM分群指标",
            ["monetary", "total_amount"]
        )

        ads_product_quantity_column = find_column(
            ads_product,
            "ADS商品指标",
            [
                "sales_count",
                "sales_quantity",
                "total_quantity",
            ]
        )

        ads_product_amount_column = find_column(
            ads_product,
            "ADS商品指标",
            [
                "sales_amount",
                "total_amount",
            ]
        )

        # =================================================
        # 18. 用户主题检查
        # =================================================

        compare(
            "有效客户数 → DWS用户数",
            valid_customer_count,
            dws_user.count()
        )

        compare(
            "有效订单数 → DWS用户订单数",
            valid_order_count,
            sum_column(
                dws_user,
                dws_user_order_column
            )
        )

        compare(
            "有效订单金额 → DWS用户金额",
            valid_order_amount,
            sum_column(
                dws_user,
                dws_user_amount_column
            ),
            tolerance=0.01
        )

        # =================================================
        # 19. 地区主题检查
        # =================================================

        compare(
            "有效订单数 → DWS地区订单数",
            valid_order_count,
            sum_column(
                dws_area,
                dws_area_order_column
            )
        )

        compare(
            "有效订单金额 → DWS地区金额",
            valid_order_amount,
            sum_column(
                dws_area,
                dws_area_amount_column
            ),
            tolerance=0.01
        )

        # =================================================
        # 20. 日销售与月度趋势检查
        # =================================================

        compare(
            "有效日期数 → ADS日销售日期数",
            valid_date_count,
            ads_sales.count()
        )

        compare(
            "有效订单数 → ADS日销售订单数",
            valid_order_count,
            sum_column(
                ads_sales,
                ads_order_column
            )
        )

        compare(
            "有效订单金额 → ADS日销售金额",
            valid_order_amount,
            sum_column(
                ads_sales,
                ads_amount_column
            ),
            tolerance=0.01
        )

        compare(
            "有效订单数 → ADS月度订单数",
            valid_order_count,
            sum_column(
                ads_monthly,
                monthly_order_column
            )
        )

        compare(
            "有效订单金额 → ADS月度销售额",
            valid_order_amount,
            sum_column(
                ads_monthly,
                monthly_amount_column
            ),
            tolerance=0.01
        )

        # =================================================
        # 21. RFM检查
        # =================================================

        compare(
            "DWS用户数 → RFM基础用户数",
            dws_user.count(),
            rfm_base.count()
        )

        compare(
            "RFM基础用户数 → RFM分群用户数",
            rfm_base.count(),
            rfm_segment.count()
        )

        compare(
            "DWS用户金额 → RFM基础金额",
            sum_column(
                dws_user,
                dws_user_amount_column
            ),
            sum_column(
                rfm_base,
                rfm_base_amount_column
            ),
            tolerance=0.01
        )

        compare(
            "RFM基础金额 → RFM分群金额",
            sum_column(
                rfm_base,
                rfm_base_amount_column
            ),
            sum_column(
                rfm_segment,
                rfm_segment_amount_column
            ),
            tolerance=0.01
        )

        # =================================================
        # 22. 商品主题检查
        # =================================================

        compare(
            "有效商品数 → DWS商品数",
            valid_product_count,
            dws_product.count()
        )

        compare(
            "有效商品销量 → DWS商品销量",
            valid_product_quantity,
            sum_column(
                dws_product,
                dws_product_quantity_column
            )
        )

        compare(
            "有效商品金额 → DWS商品金额",
            valid_product_amount,
            sum_column(
                dws_product,
                dws_product_amount_column
            ),
            tolerance=0.01
        )

        compare(
            "DWS商品数 → ADS商品数",
            dws_product.count(),
            ads_product.count()
        )

        compare(
            "DWS商品销量 → ADS商品销量",
            sum_column(
                dws_product,
                dws_product_quantity_column
            ),
            sum_column(
                ads_product,
                ads_product_quantity_column
            )
        )

        compare(
            "DWS商品金额 → ADS商品金额",
            sum_column(
                dws_product,
                dws_product_amount_column
            ),
            sum_column(
                ads_product,
                ads_product_amount_column
            ),
            tolerance=0.01
        )

        # =================================================
        # 23. 商品ABC排名检查
        # =================================================

        rank_column = find_column(
            ads_product,
            "ADS商品指标",
            [
                "sales_rank",
                "product_rank",
                "sales_amount_rank",
                "product_sales_rank",
            ]
        )

        cumulative_column = find_column(
            ads_product,
            "ADS商品指标",
            [
                "cumulative_sales_rate",
                "cumulative_sales_amount_rate",
                "cumulative_rate",
                "cumulative_sales_ratio",
            ]
        )

        abc_column = find_column(
            ads_product,
            "ADS商品指标",
            [
                "abc_level",
                "abc_class",
                "product_abc_level",
                "product_abc",
            ]
        )

        rank_metrics = (
            ads_product
            .agg(
                F.min(rank_column).alias("min_rank"),
                F.max(rank_column).alias("max_rank"),
                F.countDistinct(
                    rank_column
                ).alias("rank_count"),
                F.max(
                    cumulative_column
                ).alias("max_cumulative_rate"),
            )
            .first()
        )

        compare(
            "ABC最小排名",
            1,
            rank_metrics["min_rank"]
        )

        compare(
            "ABC最大排名",
            ads_product.count(),
            rank_metrics["max_rank"]
        )

        compare(
            "ABC排名唯一数量",
            ads_product.count(),
            rank_metrics["rank_count"]
        )

        final_cumulative_rate = to_decimal(
            rank_metrics[
                "max_cumulative_rate"
            ]
        )

        cumulative_passed = (
            abs(
                final_cumulative_rate
                - Decimal("1")
            ) <= Decimal("0.0001")
            or
            abs(
                final_cumulative_rate
                - Decimal("100")
            ) <= Decimal("0.01")
        )

        record_check(
            "ABC最终累计销售额占比",
            final_cumulative_rate,
            "1或者100%",
            cumulative_passed
        )

        abc_values = [
            row[abc_column]
            for row in (
                ads_product
                .select(abc_column)
                .distinct()
                .collect()
            )
        ]

        normalized_abc = {
            str(value).strip()[0].upper()
            for value in abc_values
            if value is not None
            and str(value).strip()
        }

        record_check(
            "ABC等级完整性",
            sorted(normalized_abc),
            ["A", "B", "C"],
            normalized_abc == {
                "A",
                "B",
                "C",
            }
        )

        # =================================================
        # 24. ABC汇总表检查
        # =================================================

        abc_summary_count_column = find_column(
            abc_summary,
            "ABC汇总表",
            [
                "product_count",
                "total_product_count",
            ]
        )

        abc_summary_amount_column = find_column(
            abc_summary,
            "ABC汇总表",
            [
                "sales_amount",
                "total_sales_amount",
            ]
        )

        compare(
            "ADS商品数 → ABC汇总商品数",
            ads_product.count(),
            sum_column(
                abc_summary,
                abc_summary_count_column
            )
        )

        compare(
            "ADS商品金额 → ABC汇总金额",
            sum_column(
                ads_product,
                ads_product_amount_column
            ),
            sum_column(
                abc_summary,
                abc_summary_amount_column
            ),
            tolerance=0.01
        )

        # =================================================
        # 25. 基础维度数量信息
        # =================================================

        print(
            "\n========== ODS基础数据量 =========="
        )

        print(
            f"订单数：{ods_order_count}"
        )

        print(
            f"订单明细数：{ods_detail_count}"
        )

        print(
            f"客户数：{ods_customer_count}"
        )

        print(
            f"商品数：{ods_product_count}"
        )

        # =================================================
        # 26. 输出最终报告
        # =================================================

        print_report()

    finally:
        spark.stop()


# =========================================================
# 27. 程序入口
# =========================================================

if __name__ == "__main__":
    main()
