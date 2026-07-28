"""
构建Spark版DWS地区销售主题。

输入：
warehouse_spark/dwd/dwd_order_detail

输出：
warehouse_spark/dws/dws_area_sales

数据颗粒度：
输出结果一行代表一个省份。
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. 项目路径
# =========================================================

# 当前文件位置：
# ECommerce_Project/spark_learning/11_build_dws_area_sales.py
CURRENT_FILE = Path(__file__).resolve()

# 项目根目录：
# ECommerce_Project
PROJECT_DIR = CURRENT_FILE.parent.parent

# DWD订单明细输入目录
INPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "dwd"
    / "dwd_order_detail"
)

# DWS地区销售主题输出目录
OUTPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "dws"
    / "dws_area_sales"
)


# =========================================================
# 2. 有效销售状态
# =========================================================

# 只有这三种状态算真正产生销售
VALID_SALES_STATUSES = [
    "已支付",
    "已发货",
    "已完成",
]


# =========================================================
# 3. 必要字段
# =========================================================

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_time",
    "order_status",
    "payable_amount",
    "receiver_province_code",
    "receiver_province_name",
]


# =========================================================
# 4. 检查必要字段
# =========================================================

def check_required_columns(dataframe):
    """
    检查DWD中是否包含地区主题需要使用的字段。
    """

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            "构建DWS地区销售主题缺少字段："
            f"{missing_columns}"
        )


# =========================================================
# 5. 主程序
# =========================================================

def main():
    """
    构建DWS地区销售主题。
    """

    spark = (
        SparkSession.builder
        .appName("BuildDwsAreaSales")
        .getOrCreate()
    )

    # 减少Spark普通运行日志
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n========== 开始构建DWS地区销售主题 ==========")
        print(f"输入目录：{INPUT_PATH}")
        print(f"输出目录：{OUTPUT_PATH}")

        # =================================================
        # 6. 读取DWD订单明细
        # =================================================

        dwd_df = spark.read.parquet(
            str(INPUT_PATH)
        )

        source_detail_count = dwd_df.count()

        print(
            f"DWD订单明细总行数："
            f"{source_detail_count}"
        )

        check_required_columns(
            dwd_df
        )

        # =================================================
        # 7. 统一字段类型
        # =================================================

        prepared_df = (
            dwd_df
            .withColumn(
                "order_id",
                F.col("order_id").cast("string")
            )
            .withColumn(
                "customer_id",
                F.col("customer_id").cast("string")
            )
            .withColumn(
                "order_status",
                F.trim(
                    F.col("order_status").cast("string")
                )
            )
            .withColumn(
                "order_time",
                F.to_timestamp("order_time")
            )
            .withColumn(
                "payable_amount",
                F.col("payable_amount").cast("decimal(20, 2)")
            )
            .withColumn(
                "receiver_province_code",
                F.col("receiver_province_code").cast("string")
            )
            .withColumn(
                "receiver_province_name",
                F.trim(
                    F.col(
                        "receiver_province_name"
                    ).cast("string")
                )
            )
        )

        # =================================================
        # 8. 过滤有效销售订单
        # =================================================

        valid_detail_df = (
            prepared_df
            .filter(
                F.col("order_status").isin(
                    VALID_SALES_STATUSES
                )
            )
        )

        valid_detail_count = valid_detail_df.count()

        excluded_detail_count = (
            source_detail_count
            - valid_detail_count
        )

        print(
            f"有效销售明细数："
            f"{valid_detail_count}"
        )

        print(
            f"排除明细数："
            f"{excluded_detail_count}"
        )

        # =================================================
        # 9. 恢复到订单颗粒度
        # =================================================

        # DWD是一行一条商品明细。
        # 同一个订单可能出现多行。
        #
        # payable_amount是整个订单的金额，
        # 所以不能直接在DWD明细上求和。
        #
        # 必须先按照order_id去重，
        # 恢复成一行一个订单。

        valid_order_df = (
            valid_detail_df
            .select(
                "order_id",
                "customer_id",
                "order_time",
                "payable_amount",
                "receiver_province_code",
                "receiver_province_name",
            )
            .dropDuplicates(
                ["order_id"]
            )
        )

        valid_order_count = valid_order_df.count()

        print(
            f"恢复订单颗粒度后的有效订单数："
            f"{valid_order_count}"
        )

        # =================================================
        # 10. 处理地区空值
        # =================================================

        area_order_df = (
            valid_order_df
            .withColumn(
                "receiver_province_code",
                F.when(
                    F.col(
                        "receiver_province_code"
                    ).isNull()
                    | (
                        F.trim(
                            F.col(
                                "receiver_province_code"
                            )
                        ) == ""
                    ),
                    F.lit("UNKNOWN")
                )
                .otherwise(
                    F.col(
                        "receiver_province_code"
                    )
                )
            )
            .withColumn(
                "receiver_province_name",
                F.when(
                    F.col(
                        "receiver_province_name"
                    ).isNull()
                    | (
                        F.trim(
                            F.col(
                                "receiver_province_name"
                            )
                        ) == ""
                    ),
                    F.lit("未知地区")
                )
                .otherwise(
                    F.col(
                        "receiver_province_name"
                    )
                )
            )
        )

        unknown_area_count = (
            area_order_df
            .filter(
                F.col(
                    "receiver_province_code"
                ) == "UNKNOWN"
            )
            .count()
        )

        print(
            f"地区信息缺失订单数："
            f"{unknown_area_count}"
        )

        # =================================================
        # 11. 按省份汇总
        # =================================================

        dws_area_df = (
            area_order_df
            .groupBy(
                "receiver_province_code",
                "receiver_province_name",
            )
            .agg(
                F.countDistinct(
                    "order_id"
                ).alias(
                    "order_count"
                ),

                F.countDistinct(
                    "customer_id"
                ).alias(
                    "customer_count"
                ),

                F.sum(
                    "payable_amount"
                ).alias(
                    "sales_amount"
                ),

                F.min(
                    "order_time"
                ).alias(
                    "first_order_time"
                ),

                F.max(
                    "order_time"
                ).alias(
                    "last_order_time"
                ),
            )
            .withColumn(
                "avg_order_amount",
                F.when(
                    F.col("order_count") > 0,
                    F.round(
                        F.col("sales_amount")
                        / F.col("order_count"),
                        2
                    )
                )
                .otherwise(
                    F.lit(0)
                )
            )
            .withColumn(
                "etl_time",
                F.current_timestamp()
            )
            .select(
                "receiver_province_code",
                "receiver_province_name",
                "order_count",
                "customer_count",
                "sales_amount",
                "avg_order_amount",
                "first_order_time",
                "last_order_time",
                "etl_time",
            )
            .orderBy(
                F.col("sales_amount").desc()
            )
        )

        area_count = dws_area_df.count()

        print(
            f"地区数量："
            f"{area_count}"
        )

        # =================================================
        # 12. 跨层指标核对
        # =================================================

        source_metrics = (
            area_order_df
            .agg(
                F.countDistinct(
                    "order_id"
                ).alias(
                    "order_count"
                ),
                F.sum(
                    "payable_amount"
                ).alias(
                    "sales_amount"
                ),
            )
            .first()
        )

        target_metrics = (
            dws_area_df
            .agg(
                F.sum(
                    "order_count"
                ).alias(
                    "order_count"
                ),
                F.sum(
                    "sales_amount"
                ).alias(
                    "sales_amount"
                ),
            )
            .first()
        )

        source_order_count = (
            source_metrics["order_count"]
            or 0
        )

        target_order_count = (
            target_metrics["order_count"]
            or 0
        )

        source_sales_amount = (
            source_metrics["sales_amount"]
            or 0
        )

        target_sales_amount = (
            target_metrics["sales_amount"]
            or 0
        )

        print("\n========== 地区主题指标核对 ==========")

        print(
            "源有效订单数："
            f"{source_order_count}"
        )

        print(
            "DWS地区订单数："
            f"{target_order_count}"
        )

        print(
            "源有效销售额："
            f"{source_sales_amount}"
        )

        print(
            "DWS地区销售额："
            f"{target_sales_amount}"
        )

        if source_order_count != target_order_count:
            raise ValueError(
                "地区主题订单数核对失败："
                f"源订单数={source_order_count}，"
                f"DWS订单数={target_order_count}"
            )

        sales_difference = abs(
            source_sales_amount
            - target_sales_amount
        )

        if sales_difference > 0.01:
            raise ValueError(
                "地区主题销售额核对失败："
                f"源销售额={source_sales_amount}，"
                f"DWS销售额={target_sales_amount}，"
                f"差异={sales_difference}"
            )

        print("地区主题订单数核对通过")
        print("地区主题销售额核对通过")

        # =================================================
        # 13. 展示结果
        # =================================================

        print("\n========== 地区销售额TOP10 ==========")

        dws_area_df.show(
            10,
            truncate=False
        )

        # =================================================
        # 14. 保存Parquet
        # =================================================

        (
            dws_area_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(
                str(OUTPUT_PATH)
            )
        )

        print(
            "\nDWS地区销售主题保存完成："
            f"{OUTPUT_PATH}"
        )

        print(
            f"输出地区数：{area_count}"
        )

        print(
            "========== DWS地区销售主题构建完成 ==========\n"
        )

    finally:
        spark.stop()


# =========================================================
# 15. 程序入口
# =========================================================

if __name__ == "__main__":
    main()
