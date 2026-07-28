from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. 路径和业务配置
# =========================================================

PROJECT_DIR = Path(__file__).resolve().parents[1]

DWD_INPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "dwd"
    / "dwd_order_detail"
)

ADS_OUTPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ads"
    / "ads_sales_summary"
)


VALID_SALES_STATUSES = [
    "已支付",
    "已发货",
    "已完成",
]


# =========================================================
# 2. 创建Spark
# =========================================================

def create_spark():
    spark = (
        SparkSession.builder
        .appName("BuildAdsSalesSummary")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# =========================================================
# 3. 读取DWD
# =========================================================

def read_dwd(spark):
    print(f"DWD读取目录：{DWD_INPUT_PATH}")

    dwd_df = spark.read.parquet(
        str(DWD_INPUT_PATH)
    )

    return dwd_df


# =========================================================
# 4. 恢复有效订单颗粒度
# =========================================================

def build_valid_order_df(dwd_df):
    all_order_count = (
        dwd_df
        .select("order_id")
        .distinct()
        .count()
    )

    valid_order_df = (
        dwd_df

        # 只保留有效销售订单
        .filter(
            F.col("order_status").isin(
                VALID_SALES_STATUSES
            )
        )

        # 只选择每日销售指标需要的字段
        .select(
            "order_id",
            "customer_id",
            "order_time",
            "payable_amount",
            "coupon_amount",
            "freight_amount",
        )

        # 同一个订单只保留一行
        .dropDuplicates(
            ["order_id"]
        )

        # 从订单时间中提取订单日期
        .withColumn(
            "order_date",
            F.to_date(
                F.col("order_time")
            ),
        )

        .cache()
    )

    valid_order_count = (
        valid_order_df.count()
    )

    excluded_order_count = (
        all_order_count
        - valid_order_count
    )

    invalid_date_count = (
        valid_order_df
        .filter(
            F.col("order_date").isNull()
        )
        .count()
    )

    print("\n========== ADS销售口径 ==========")
    print(f"全部订单数：{all_order_count}")
    print(f"有效订单数：{valid_order_count}")
    print(f"排除订单数：{excluded_order_count}")
    print(f"订单日期无效数：{invalid_date_count}")

    if invalid_date_count != 0:
        raise ValueError(
            "存在无法转换订单日期的有效订单"
        )

    return valid_order_df


# =========================================================
# 5. 按日期汇总销售指标
# =========================================================

def build_sales_summary(
        valid_order_df,
):
    ads_sales_df = (
        valid_order_df

        # 把同一天的订单放在一起
        .groupBy(
            "order_date"
        )

        # 计算每天的经营指标
        .agg(
            # 当天有效订单数量
            F.countDistinct(
                "order_id"
            ).alias(
                "order_count"
            ),

            # 当天有效客户数量
            F.countDistinct(
                "customer_id"
            ).alias(
                "customer_count"
            ),

            # 当天有效销售额
            F.sum(
                "payable_amount"
            ).alias(
                "sales_amount"
            ),

            # 当天优惠券金额
            F.sum(
                "coupon_amount"
            ).alias(
                "coupon_amount"
            ),

            # 当天运费金额
            F.sum(
                "freight_amount"
            ).alias(
                "freight_amount"
            ),
        )

        # 计算每天的平均客单价
        .withColumn(
            "avg_order_amount",
            F.when(
                F.col("order_count") > 0,
                F.round(
                    F.col("sales_amount")
                    / F.col("order_count"),
                    2,
                ),
            ).otherwise(
                F.lit(0)
            ),
        )

        # 从日期中提取年份
        .withColumn(
            "year",
            F.year(
                F.col("order_date")
            ),
        )

        # 从日期中提取月份
        .withColumn(
            "month",
            F.month(
                F.col("order_date")
            ),
        )

        # 生成2025-01这样的年月
        .withColumn(
            "year_month",
            F.date_format(
                F.col("order_date"),
                "yyyy-MM",
            ),
        )

        # 增加ETL加工信息
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

        # 重新排列字段顺序
        .select(
            "order_date",
            "year",
            "month",
            "year_month",
            "order_count",
            "customer_count",
            "sales_amount",
            "avg_order_amount",
            "coupon_amount",
            "freight_amount",
            "etl_time",
            "source_system",
            "etl_batch",
        )

        .cache()
    )

    return ads_sales_df


# =========================================================
# 6. 跨层指标核对
# =========================================================

def check_sales_summary(
        valid_order_df,
        ads_sales_df,
):
    # DWD有效订单正确值
    source_result = (
        valid_order_df
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

    # ADS每日指标合计
    target_result = (
        ads_sales_df
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
        source_result["order_count"]
    )

    target_order_count = (
        target_result["order_count"]
    )

    source_sales_amount = (
        source_result["sales_amount"]
        or Decimal("0.00")
    )

    target_sales_amount = (
        target_result["sales_amount"]
        or Decimal("0.00")
    )

    amount_difference = abs(
        source_sales_amount
        - target_sales_amount
    )

    date_count = ads_sales_df.count()

    duplicate_date_count = (
        ads_sales_df
        .groupBy("order_date")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    date_result = (
        ads_sales_df
        .agg(
            F.min(
                "order_date"
            ).alias(
                "first_date"
            ),

            F.max(
                "order_date"
            ).alias(
                "last_date"
            ),
        )
        .first()
    )

    first_date = date_result["first_date"]
    last_date = date_result["last_date"]

    expected_date_count = (
        last_date - first_date
    ).days + 1

    missing_date_count = (
        expected_date_count
        - date_count
    )

    print("\n========== ADS指标核对 ==========")
    print(f"ADS统计日期数：{date_count}")
    print(f"最早统计日期：{first_date}")
    print(f"最晚统计日期：{last_date}")

    print(
        "日期范围理论天数："
        f"{expected_date_count}"
    )

    print(
        "没有销售记录的日期数："
        f"{missing_date_count}"
    )

    print(
        "DWD有效订单数："
        f"{source_order_count}"
    )

    print(
        "ADS订单数合计："
        f"{target_order_count}"
    )

    print(
        "DWD有效销售金额："
        f"{source_sales_amount}"
    )

    print(
        "ADS销售金额合计："
        f"{target_sales_amount}"
    )

    print(
        "销售金额差异："
        f"{amount_difference}"
    )

    print(
        "重复统计日期数量："
        f"{duplicate_date_count}"
    )

    if (
        source_order_count
        != target_order_count
    ):
        raise ValueError(
            "DWD订单数与ADS订单数不一致"
        )

    if (
        amount_difference
        > Decimal("0.01")
    ):
        raise ValueError(
            "DWD销售金额与ADS销售金额不一致"
        )

    if duplicate_date_count != 0:
        raise ValueError(
            "ADS存在重复统计日期"
        )

    print("ADS每日销售指标核对通过")


# =========================================================
# 7. 保存并回读
# =========================================================

def save_and_check(ads_sales_df):
    before_save_count = (
        ads_sales_df.count()
    )

    ADS_OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    (
        ads_sales_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(
            str(ADS_OUTPUT_PATH)
        )
    )

    saved_df = (
        ads_sales_df.sparkSession
        .read
        .parquet(
            str(ADS_OUTPUT_PATH)
        )
    )

    after_save_count = (
        saved_df.count()
    )
    print("\n========== ADS保存检查 ==========")
    print(f"保存前日期数：{before_save_count}")
    print(f"保存后日期数：{after_save_count}")
    print(f"输出目录：{ADS_OUTPUT_PATH}")

    if (
        before_save_count
        != after_save_count
    ):
        raise ValueError(
            "ADS保存前后日期数量不一致"
        )

    print("ADS每日销售指标保存成功")


# =========================================================
# 8. 主程序
# =========================================================

def main():
    spark = create_spark()

    valid_order_df = None
    ads_sales_df = None

    try:
        print("开始构建Spark ADS每日销售指标")

        dwd_df = read_dwd(spark)

        print("\n========== DWD基本信息 ==========")
        print(f"DWD明细行数：{dwd_df.count()}")

        valid_order_df = (
            build_valid_order_df(
                dwd_df
            )
        )

        ads_sales_df = (
            build_sales_summary(
                valid_order_df
            )
        )

        print("\n========== ADS字段结构 ==========")
        ads_sales_df.printSchema()

        print("\n========== ADS每日销售数据 ==========")
        (
            ads_sales_df
            .orderBy("order_date")
            .show(
                20,
                truncate=False,
            )
        )

        check_sales_summary(
            valid_order_df,
            ads_sales_df,
        )

        save_and_check(
            ads_sales_df
        )

        print("\nSpark ADS每日销售指标构建完成")

    finally:
        if valid_order_df is not None:
            valid_order_df.unpersist()

        if ads_sales_df is not None:
            ads_sales_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
