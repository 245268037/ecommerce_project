from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. 路径配置
# =========================================================

PROJECT_DIR = Path(__file__).resolve().parents[1]

DWS_USER_INPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "dws"
    / "dws_user_sales"
)

RFM_OUTPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ads"
    / "ads_user_rfm_base"
)


# =========================================================
# 2. 创建Spark
# =========================================================

def create_spark():
    spark = (
        SparkSession.builder
        .appName("BuildAdsUserRfmBase")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# =========================================================
# 3. 读取DWS用户主题
# =========================================================

def read_dws_user(spark):
    print(
        "DWS用户主题读取目录："
        f"{DWS_USER_INPUT_PATH}"
    )

    dws_user_df = (
        spark.read.parquet(
            str(DWS_USER_INPUT_PATH)
        )
        .cache()
    )

    return dws_user_df


# =========================================================
# 4. 确定RFM分析基准日期
# =========================================================

def get_rfm_reference_date(
        dws_user_df,
):
    result = (
        dws_user_df
        .agg(
            F.max(
                F.to_date(
                    F.col("last_order_time")
                )
            ).alias(
                "max_order_date"
            )
        )
        .first()
    )

    max_order_date = (
        result["max_order_date"]
    )

    if max_order_date is None:
        raise ValueError(
            "无法获得最大订单日期"
        )

    # 最大订单日期的下一天作为分析日期
    reference_date = (
        max_order_date
        + timedelta(days=1)
    )

    print("\n========== RFM分析日期 ==========")
    print(f"最大订单日期：{max_order_date}")
    print(f"RFM分析基准日期：{reference_date}")

    return reference_date


# =========================================================
# 5. 构建RFM基础指标
# =========================================================

def build_rfm_base(
        dws_user_df,
        reference_date,
):
    rfm_df = (
        dws_user_df
        .select(
            # 用户基本信息
            "customer_id",
            "customer_name",
            "gender",
            "age",
            "member_level",
            "customer_province_code",
            "customer_province_name",

            # 用户订单时间
            "first_order_time",
            "last_order_time",

            # R：距离最近消费过去多少天
            F.datediff(
                F.lit(
                    reference_date
                ).cast("date"),
                F.to_date(
                    F.col(
                        "last_order_time"
                    )
                ),
            ).alias(
                "recency_days"
            ),

            # F：用户有效订单数量
            F.col(
                "order_count"
            ).alias(
                "frequency"
            ),

            # M：用户有效订单消费金额
            F.col(
                "total_amount"
            ).alias(
                "monetary"
            ),
        )

        # 保存分析基准日期
        .withColumn(
            "rfm_reference_date",
            F.lit(
                reference_date
            ).cast("date"),
        )

        # 增加ETL信息
        .withColumn(
            "etl_time",
            F.current_timestamp(),
        )

        .withColumn(
            "source_system",
            F.lit("ecommerce"),
        )

        .withColumn(
            "etl_batch",
            F.date_format(
                F.current_timestamp(),
                "yyyyMMddHHmmss",
            ),
        )

        .cache()
    )

    return rfm_df


# =========================================================
# 6. RFM基础指标检查
# =========================================================

def check_rfm_base(
        dws_user_df,
        rfm_df,
):
    # DWS用户主题正确值
    source_result = (
        dws_user_df
        .agg(
            F.count(
                F.lit(1)
            ).alias(
                "user_count"
            ),

            F.sum(
                "total_amount"
            ).alias(
                "total_amount"
            ),
        )
        .first()
    )

    # RFM基础表统计结果
    target_result = (
        rfm_df
        .agg(
            F.count(
                F.lit(1)
            ).alias(
                "user_count"
            ),

            F.countDistinct(
                "customer_id"
            ).alias(
                "distinct_user_count"
            ),

            F.sum(
                "monetary"
            ).alias(
                "total_amount"
            ),

            F.sum(
                F.when(
                    F.col("recency_days") < 0,
                    1,
                ).otherwise(0)
            ).alias(
                "invalid_recency_count"
            ),

            F.sum(
                F.when(
                    F.col("frequency") <= 0,
                    1,
                ).otherwise(0)
            ).alias(
                "invalid_frequency_count"
            ),

            F.sum(
                F.when(
                    F.col("monetary") < 0,
                    1,
                ).otherwise(0)
            ).alias(
                "invalid_monetary_count"
            ),
        )
        .first()
    )

    source_user_count = (
        source_result["user_count"]
    )

    target_user_count = (
        target_result["user_count"]
    )

    distinct_user_count = (
        target_result[
            "distinct_user_count"
        ]
    )

    duplicate_user_count = (
        target_user_count
        - distinct_user_count
    )

    source_total_amount = (
        source_result["total_amount"]
        or Decimal("0.00")
    )

    target_total_amount = (
        target_result["total_amount"]
        or Decimal("0.00")
    )

    amount_difference = abs(
        source_total_amount
        - target_total_amount
    )

    invalid_recency_count = (
        target_result[
            "invalid_recency_count"
        ] or 0
    )

    invalid_frequency_count = (
        target_result[
            "invalid_frequency_count"
        ] or 0
    )

    invalid_monetary_count = (
        target_result[
            "invalid_monetary_count"
        ] or 0
    )

    print("\n========== RFM基础指标检查 ==========")
    print(f"DWS用户数：{source_user_count}")
    print(f"RFM用户数：{target_user_count}")
    print(f"重复用户数：{duplicate_user_count}")

    print(
        "DWS用户消费金额："
        f"{source_total_amount}"
    )

    print(
        "RFM消费金额："
        f"{target_total_amount}"
    )

    print(
        "金额差异："
        f"{amount_difference}"
    )

    print(
        "R值异常用户数："
        f"{invalid_recency_count}"
    )

    print(
        "F值异常用户数："
        f"{invalid_frequency_count}"
    )

    print(
        "M值异常用户数："
        f"{invalid_monetary_count}"
    )

    if (
        source_user_count
        != target_user_count
    ):
        raise ValueError(
            "DWS用户数与RFM用户数不一致"
        )

    if duplicate_user_count != 0:
        raise ValueError(
            "RFM基础表存在重复用户"
        )

    if (
        amount_difference
        > Decimal("0.01")
    ):
        raise ValueError(
            "DWS金额与RFM金额不一致"
        )

    if invalid_recency_count != 0:
        raise ValueError(
            "RFM存在小于0的最近消费间隔"
        )

    if invalid_frequency_count != 0:
        raise ValueError(
            "RFM存在小于或等于0的购买次数"
        )

    if invalid_monetary_count != 0:
        raise ValueError(
            "RFM存在小于0的消费金额"
        )

    print("RFM基础指标检查全部通过")


# =========================================================
# 7. 保存和回读
# =========================================================

def save_and_check(rfm_df):
    before_save_count = (
        rfm_df.count()
    )

    RFM_OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    (
        rfm_df
        .write
        .mode("overwrite")
        .parquet(
            str(RFM_OUTPUT_PATH)
        )
    )

    saved_df = (
        rfm_df.sparkSession
        .read
        .parquet(
            str(RFM_OUTPUT_PATH)
        )
    )

    after_save_count = (
        saved_df.count()
    )

    print("\n========== RFM保存检查 ==========")
    print(f"保存前用户数：{before_save_count}")
    print(f"保存后用户数：{after_save_count}")
    print(f"输出目录：{RFM_OUTPUT_PATH}")

    if (
        before_save_count
        != after_save_count
    ):
        raise ValueError(
            "RFM保存前后用户数量不一致"
        )

    print("RFM基础指标保存成功")


# =========================================================
# 8. 主程序
# =========================================================

def main():
    spark = create_spark()

    dws_user_df = None
    rfm_df = None

    try:
        print("开始构建Spark RFM基础指标")

        dws_user_df = read_dws_user(
            spark
        )

        print("\n========== DWS用户主题 ==========")
        print(
            "DWS用户数量："
            f"{dws_user_df.count()}"
        )

        reference_date = (
            get_rfm_reference_date(
                dws_user_df
            )
        )

        rfm_df = build_rfm_base(
            dws_user_df,
            reference_date,
        )

        print("\n========== RFM字段结构 ==========")
        rfm_df.printSchema()

        print("\n========== RFM指标统计 ==========")
        (
            rfm_df
            .select(
                "recency_days",
                "frequency",
                "monetary",
            )
            .summary(
                "count",
                "mean",
                "min",
                "25%",
                "50%",
                "75%",
                "max",
            )
            .show(
                truncate=False
            )
        )

        print("\n========== 高价值用户示例 ==========")
        (
            rfm_df
            .orderBy(
                F.col("monetary").desc()
            )
            .show(
                10,
                truncate=False,
            )
        )

        check_rfm_base(
            dws_user_df,
            rfm_df,
        )

        save_and_check(
            rfm_df
        )

        print("\nSpark RFM基础指标构建完成")

    finally:
        if dws_user_df is not None:
            dws_user_df.unpersist()

        if rfm_df is not None:
            rfm_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
