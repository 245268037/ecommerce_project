"""
学习Spark执行计划与Shuffle。

本程序对比三种操作：

1. filter和select：
   通常不需要重新搬运数据。

2. groupBy：
   需要把相同省份的订单放到一起，
   通常会产生Exchange和Shuffle。

3. orderBy：
   需要把全部结果重新排序，
   通常也会产生Exchange和Shuffle。
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. 项目路径
# =========================================================

CURRENT_FILE = Path(__file__).resolve()
PROJECT_DIR = CURRENT_FILE.parent.parent

INPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ods"
    / "ods_order"
)


# =========================================================
# 2. 有效销售状态
# =========================================================

VALID_SALES_STATUSES = [
    "已支付",
    "已发货",
    "已完成",
]


# =========================================================
# 3. 主程序
# =========================================================

def main():
    spark = (
        SparkSession.builder
        .appName(
            "SparkExecutionPlanLearning"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:
        print(
            "\n========== Spark执行计划学习 =========="
        )

        print(
            f"输入目录：{INPUT_PATH}"
        )

        # =============================================
        # 4. 读取ODS订单表
        # =============================================

        order_df = (
            spark.read.parquet(
                str(INPUT_PATH)
            )
        )

        print(
            "ODS原始分区数：",
            order_df.rdd.getNumPartitions()
        )

        print(
            "ODS订单总数：",
            order_df.count()
        )

        # =============================================
        # 5. 只过滤和选择字段
        # =============================================

        valid_order_df = (
            order_df
            .filter(
                F.col("order_status").isin(
                    VALID_SALES_STATUSES
                )
            )
            .select(
                "order_id",
                "customer_id",
                "receiver_province_code",
                "receiver_province_name",
                F.col(
                    "payable_amount"
                )
                .cast(
                    "decimal(20, 2)"
                )
                .alias(
                    "payable_amount"
                ),
            )
        )

        print(
            "\n========== 计划一：过滤与选择字段 =========="
        )

        valid_order_df.explain(
            mode="formatted"
        )

        print(
            "有效订单分区数：",
            valid_order_df.rdd.getNumPartitions()
        )

        # =============================================
        # 6. 按省份进行汇总
        # =============================================

        area_summary_df = (
            valid_order_df
            .groupBy(
                "receiver_province_code",
                "receiver_province_name",
            )
            .agg(
                F.count(
                    "*"
                ).alias(
                    "order_count"
                ),

                F.sum(
                    "payable_amount"
                ).alias(
                    "sales_amount"
                ),
            )
        )

        print(
            "\n========== 计划二：按省份分组 =========="
        )

        area_summary_df.explain(
            mode="formatted"
        )

        print(
            "地区汇总分区数：",
            area_summary_df.rdd.getNumPartitions()
        )

        # =============================================
        # 7. 按销售额从高到低排序
        # =============================================

        sorted_area_df = (
            area_summary_df
            .orderBy(
                F.col(
                    "sales_amount"
                ).desc()
            )
        )

        print(
            "\n========== 计划三：销售额排序 =========="
        )

        sorted_area_df.explain(
            mode="formatted"
        )

        print(
            "排序结果分区数：",
            sorted_area_df.rdd.getNumPartitions()
        )

        # =============================================
        # 8. 触发真正计算
        # =============================================

        print(
            "\n========== 地区销售额TOP10 =========="
        )

        sorted_area_df.show(
            10,
            truncate=False
        )

        print(
            "\nSpark执行计划学习完成"
        )

    finally:
        spark.stop()


# =========================================================
# 9. 程序入口
# =========================================================

if __name__ == "__main__":
    main()
