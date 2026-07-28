from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =========================================================
# 1. 路径配置
# =========================================================

PROJECT_DIR = Path(__file__).resolve().parents[1]

DAILY_ADS_INPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ads"
    / "ads_sales_summary"
)

MONTHLY_ADS_OUTPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ads"
    / "ads_monthly_sales_trend"
)


# =========================================================
# 2. 创建Spark
# =========================================================

def create_spark():
    spark = (
        SparkSession.builder
        .appName("BuildAdsMonthlySalesTrend")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# =========================================================
# 3. 读取每日销售指标
# =========================================================

def read_daily_ads(spark):
    print(
        "读取每日ADS："
        f"{DAILY_ADS_INPUT_PATH}"
    )

    daily_df = spark.read.parquet(
        str(DAILY_ADS_INPUT_PATH)
    )

    return daily_df


# =========================================================
# 4. 汇总月度销售指标
# =========================================================

def build_monthly_base(daily_df):
    monthly_df = (
        daily_df

        # 把同一个月的数据放在一起
        .groupBy(
            "year_month"
        )

        # 统计每个月的数据
        .agg(
            # 取出当前月份对应的年份
            F.first(
                "year"
            ).alias(
                "year"
            ),

            # 取出月份数字
            F.first(
                "month"
            ).alias(
                "month"
            ),

            # 当月有销售记录的天数
            F.countDistinct(
                "order_date"
            ).alias(
                "sales_days"
            ),

            # 每日订单数相加
            F.sum(
                "order_count"
            ).alias(
                "order_count"
            ),

            # 每日销售额相加
            F.sum(
                "sales_amount"
            ).alias(
                "sales_amount"
            ),

            # 每日优惠券金额相加
            F.sum(
                "coupon_amount"
            ).alias(
                "coupon_amount"
            ),

            # 每日运费相加
            F.sum(
                "freight_amount"
            ).alias(
                "freight_amount"
            ),
        )

        # 重新计算月度客单价
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
    )

    return monthly_df


# =========================================================
# 5. 增加上月数据、环比和累计值
# =========================================================

def add_trend_metrics(monthly_df):
    # 按月份从小到大排队
    month_window = (
        Window
        .orderBy("year_month")
    )

    # 从第一行一直计算到当前行
    cumulative_window = (
        Window
        .orderBy("year_month")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow,
        )
    )

    trend_df = (
        monthly_df

        # 找到上个月销售额
        .withColumn(
            "previous_month_sales_amount",
            F.lag(
                "sales_amount",
                1,
            ).over(
                month_window
            ),
        )

        # 找到上个月订单数
        .withColumn(
            "previous_month_order_count",
            F.lag(
                "order_count",
                1,
            ).over(
                month_window
            ),
        )

        # 计算销售额环比
        .withColumn(
            "sales_amount_mom_rate",
            F.when(
                F.col(
                    "previous_month_sales_amount"
                ).isNull(),
                F.lit(None),
            ).when(
                F.col(
                    "previous_month_sales_amount"
                ) == 0,
                F.lit(None),
            ).otherwise(
                F.round(
                    (
                        F.col("sales_amount")
                        - F.col(
                            "previous_month_sales_amount"
                        )
                    )
                    / F.col(
                        "previous_month_sales_amount"
                    ),
                    4,
                )
            ),
        )

        # 计算订单数环比
        .withColumn(
            "order_count_mom_rate",
            F.when(
                F.col(
                    "previous_month_order_count"
                ).isNull(),
                F.lit(None),
            ).when(
                F.col(
                    "previous_month_order_count"
                ) == 0,
                F.lit(None),
            ).otherwise(
                F.round(
                    (
                        F.col("order_count")
                        - F.col(
                            "previous_month_order_count"
                        )
                    )
                    / F.col(
                        "previous_month_order_count"
                    ),
                    4,
                )
            ),
        )

        # 计算累计销售额
        .withColumn(
            "cumulative_sales_amount",
            F.sum(
                "sales_amount"
            ).over(
                cumulative_window
            ),
        )

        # 计算累计订单数
        .withColumn(
            "cumulative_order_count",
            F.sum(
                "order_count"
            ).over(
                cumulative_window
            ),
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

        # 重新排列字段顺序
        .select(
            "year_month",
            "year",
            "month",
            "sales_days",
            "order_count",
            "sales_amount",
            "avg_order_amount",
            "coupon_amount",
            "freight_amount",
            "previous_month_order_count",
            "previous_month_sales_amount",
            "order_count_mom_rate",
            "sales_amount_mom_rate",
            "cumulative_order_count",
            "cumulative_sales_amount",
            "etl_time",
            "source_system",
            "etl_batch",
        )

        .cache()
    )

    return trend_df


# =========================================================
# 6. 指标核对
# =========================================================

def check_monthly_trend(
        daily_df,
        monthly_df,
):
    # 每日ADS的总指标
    source_result = (
        daily_df
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

    # 月度ADS的总指标
    target_result = (
        monthly_df
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
        source_result["order_count"] or 0
    )

    target_order_count = (
        target_result["order_count"] or 0
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

    month_count = monthly_df.count()

    duplicate_month_count = (
        monthly_df
        .groupBy("year_month")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    # 找出最后一个月
    last_month_result = (
        monthly_df
        .orderBy(
            F.col("year_month").desc()
        )
        .select(
            "year_month",
            "cumulative_order_count",
            "cumulative_sales_amount",
        )
        .first()
    )

    last_cumulative_order_count = (
        last_month_result[
            "cumulative_order_count"
        ]
    )

    last_cumulative_sales_amount = (
        last_month_result[
            "cumulative_sales_amount"
        ]
    )

    cumulative_amount_difference = abs(
        source_sales_amount
        - last_cumulative_sales_amount
    )

    print("\n========== 月度指标核对 ==========")
    print(f"月度记录数量：{month_count}")

    print(
        "每日ADS订单数合计："
        f"{source_order_count}"
    )

    print(
        "月度ADS订单数合计："
        f"{target_order_count}"
    )

    print(
        "每日ADS销售额合计："
        f"{source_sales_amount}"
    )

    print(
        "月度ADS销售额合计："
        f"{target_sales_amount}"
    )

    print(
        "月度销售额差异："
        f"{amount_difference}"
    )

    print(
        "重复月份数量："
        f"{duplicate_month_count}"
    )

    print(
        "最后统计月份："
        f"{last_month_result['year_month']}"
    )

    print(
        "最后一个月累计订单数："
        f"{last_cumulative_order_count}"
    )

    print(
        "最后一个月累计销售额："
        f"{last_cumulative_sales_amount}"
    )

    if (
        source_order_count
        != target_order_count
    ):
        raise ValueError(
            "每日ADS与月度ADS订单数不一致"
        )

    if (
        amount_difference
        > Decimal("0.01")
    ):
        raise ValueError(
            "每日ADS与月度ADS销售额不一致"
        )

    if duplicate_month_count != 0:
        raise ValueError(
            "月度ADS存在重复月份"
        )

    if (
        last_cumulative_order_count
        != source_order_count
    ):
        raise ValueError(
            "累计订单数核对失败"
        )

    if (
        cumulative_amount_difference
        > Decimal("0.01")
    ):
        raise ValueError(
            "累计销售额核对失败"
        )

    print("ADS月度销售趋势指标核对通过")


# =========================================================
# 7. 保存和回读检查
# =========================================================

def save_and_check(monthly_df):
    before_save_count = (
        monthly_df.count()
    )

    MONTHLY_ADS_OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    (
        monthly_df
        .write
        .mode("overwrite")
        .parquet(
            str(MONTHLY_ADS_OUTPUT_PATH)
        )
    )

    saved_df = (
        monthly_df.sparkSession
        .read
        .parquet(
            str(MONTHLY_ADS_OUTPUT_PATH)
        )
    )

    after_save_count = (
        saved_df.count()
    )

    print("\n========== 保存结果检查 ==========")
    print(f"保存前月份数：{before_save_count}")
    print(f"保存后月份数：{after_save_count}")

    print(
        "输出目录："
        f"{MONTHLY_ADS_OUTPUT_PATH}"
    )

    if (
        before_save_count
        != after_save_count
    ):
        raise ValueError(
            "月度ADS保存前后数量不一致"
        )

    print("ADS月度销售趋势保存成功")


# =========================================================
# 8. 主程序
# =========================================================

def main():
    spark = create_spark()

    monthly_df = None

    try:
        print("开始构建Spark ADS月度销售趋势")

        daily_df = read_daily_ads(spark)

        print("\n========== 每日ADS信息 ==========")
        print(f"每日记录数：{daily_df.count()}")

        monthly_base_df = (
            build_monthly_base(
                daily_df
            )
        )

        monthly_df = (
            add_trend_metrics(
                monthly_base_df
            )
        )

        print("\n========== 月度趋势字段结构 ==========")
        monthly_df.printSchema()

        print("\n========== 月度销售趋势 ==========")
        (
            monthly_df
            .orderBy("year_month")
            .show(
                20,
                truncate=False,
            )
        )

        check_monthly_trend(
            daily_df,
            monthly_df,
        )

        save_and_check(
            monthly_df
        )

        print("\nSpark ADS月度销售趋势构建完成")

    finally:
        if monthly_df is not None:
            monthly_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
