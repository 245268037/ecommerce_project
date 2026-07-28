from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =========================================================
# 1. 路径配置
# =========================================================

PROJECT_DIR = Path(__file__).resolve().parents[1]

RFM_BASE_INPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ads"
    / "ads_user_rfm_base"
)

RFM_SEGMENT_OUTPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ads"
    / "ads_user_rfm_segment"
)

RFM_SUMMARY_OUTPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ads"
    / "ads_user_rfm_segment_summary"
)


# =========================================================
# 2. 创建Spark
# =========================================================

def create_spark():
    spark = (
        SparkSession.builder
        .appName("BuildAdsUserRfmSegment")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# =========================================================
# 3. 读取RFM基础表
# =========================================================

def read_rfm_base(spark):
    print(
        "RFM基础表读取目录："
        f"{RFM_BASE_INPUT_PATH}"
    )

    rfm_base_df = (
        spark.read
        .parquet(
            str(RFM_BASE_INPUT_PATH)
        )
        .cache()
    )

    return rfm_base_df


# =========================================================
# 4. 计算R、F、M评分
# =========================================================

def add_rfm_scores(rfm_base_df):
    # R值：间隔天数越大越差
    # 所以从大到小排列
    # 最差的人在前面拿1分
    # 最好的人在后面拿5分
    r_window = (
        Window.orderBy(
            F.col("recency_days").desc(),
            F.col("customer_id").asc(),
        )
    )

    # F值：购买次数越大越好
    # 从小到大排列
    # 购买少的在前面拿1分
    # 购买多的在后面拿5分
    f_window = (
        Window.orderBy(
            F.col("frequency").asc(),
            F.col("customer_id").asc(),
        )
    )

    # M值：消费金额越大越好
    # 从小到大排列
    # 消费少的在前面拿1分
    # 消费多的在后面拿5分
    m_window = (
        Window.orderBy(
            F.col("monetary").asc(),
            F.col("customer_id").asc(),
        )
    )

    scored_df = (
        rfm_base_df

        # R评分
        .withColumn(
            "r_score",
            F.ntile(5).over(
                r_window
            ),
        )

        # F评分
        .withColumn(
            "f_score",
            F.ntile(5).over(
                f_window
            ),
        )

        # M评分
        .withColumn(
            "m_score",
            F.ntile(5).over(
                m_window
            ),
        )

        # RFM综合分数
        .withColumn(
            "rfm_total_score",
            F.col("r_score")
            + F.col("f_score")
            + F.col("m_score"),
        )
    )

    return scored_df


# =========================================================
# 5. 根据评分划分用户类型
# =========================================================

def add_user_segment(scored_df):
    segment_df = (
        scored_df

        .withColumn(
            "user_segment",

            # R高、F高、M高
            F.when(
                (
                    F.col("r_score") >= 4
                )
                &
                (
                    F.col("f_score") >= 4
                )
                &
                (
                    F.col("m_score") >= 4
                ),
                "重要价值用户",
            )

            # R低、F高、M高
            .when(
                (
                    F.col("r_score") < 4
                )
                &
                (
                    F.col("f_score") >= 4
                )
                &
                (
                    F.col("m_score") >= 4
                ),
                "重要保持用户",
            )

            # R高、F低、M高
            .when(
                (
                    F.col("r_score") >= 4
                )
                &
                (
                    F.col("f_score") < 4
                )
                &
                (
                    F.col("m_score") >= 4
                ),
                "重要发展用户",
            )

            # R低、F低、M高
            .when(
                (
                    F.col("r_score") < 4
                )
                &
                (
                    F.col("f_score") < 4
                )
                &
                (
                    F.col("m_score") >= 4
                ),
                "重要挽留用户",
            )

            # R高、F高、M低
            .when(
                (
                    F.col("r_score") >= 4
                )
                &
                (
                    F.col("f_score") >= 4
                )
                &
                (
                    F.col("m_score") < 4
                ),
                "一般价值用户",
            )

            # R低、F高、M低
            .when(
                (
                    F.col("r_score") < 4
                )
                &
                (
                    F.col("f_score") >= 4
                )
                &
                (
                    F.col("m_score") < 4
                ),
                "一般保持用户",
            )

            # R高、F低、M低
            .when(
                (
                    F.col("r_score") >= 4
                )
                &
                (
                    F.col("f_score") < 4
                )
                &
                (
                    F.col("m_score") < 4
                ),
                "一般发展用户",
            )

            # R低、F低、M低
            .when(
                (
                    F.col("r_score") < 4
                )
                &
                (
                    F.col("f_score") < 4
                )
                &
                (
                    F.col("m_score") < 4
                ),
                "一般挽留用户",
            )

            # 前面都没有匹配时
            .otherwise(
                "未分类"
            ),
        )

        # 更新本次加工信息
        .withColumn(
            "segment_etl_time",
            F.current_timestamp(),
        )

        .cache()
    )

    return segment_df


# =========================================================
# 6. 构建用户分群汇总表
# =========================================================

def build_segment_summary(segment_df):
    total_user_count = (
        segment_df.count()
    )

    summary_df = (
        segment_df

        # 相同用户类型放到一起
        .groupBy(
            "user_segment"
        )

        # 计算每种用户类型的指标
        .agg(
            # 用户数量
            F.countDistinct(
                "customer_id"
            ).alias(
                "user_count"
            ),

            # 平均最近消费间隔
            F.round(
                F.avg(
                    "recency_days"
                ),
                2,
            ).alias(
                "avg_recency"
            ),

            # 平均购买次数
            F.round(
                F.avg(
                    "frequency"
                ),
                2,
            ).alias(
                "avg_frequency"
            ),

            # 分群消费总额
            F.sum(
                "monetary"
            ).alias(
                "total_monetary"
            ),

            # 用户平均消费金额
            F.round(
                F.avg(
                    "monetary"
                ),
                2,
            ).alias(
                "avg_monetary"
            ),

            # 平均R分
            F.round(
                F.avg(
                    "r_score"
                ),
                2,
            ).alias(
                "avg_r_score"
            ),

            # 平均F分
            F.round(
                F.avg(
                    "f_score"
                ),
                2,
            ).alias(
                "avg_f_score"
            ),

            # 平均M分
            F.round(
                F.avg(
                    "m_score"
                ),
                2,
            ).alias(
                "avg_m_score"
            ),
        )

        # 计算各分群用户占比
        .withColumn(
            "user_rate",
            F.round(
                F.col("user_count")
                / F.lit(total_user_count),
                4,
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

        .cache()
    )

    return summary_df


# =========================================================
# 7. 检查评分和分群结果
# =========================================================

def check_segment_result(
        rfm_base_df,
        segment_df,
        summary_df,
):
    # RFM基础表正确值
    source_result = (
        rfm_base_df
        .agg(
            F.count(
                F.lit(1)
            ).alias(
                "user_count"
            ),

            F.sum(
                "monetary"
            ).alias(
                "total_monetary"
            ),
        )
        .first()
    )

    # 分群明细统计
    segment_result = (
        segment_df
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
                "total_monetary"
            ),

            # 任意一个评分不在1～5范围
            F.sum(
                F.when(
                    F.col("r_score").isNull()
                    |
                    (
                        ~F.col(
                            "r_score"
                        ).between(1, 5)
                    )
                    |
                    F.col("f_score").isNull()
                    |
                    (
                        ~F.col(
                            "f_score"
                        ).between(1, 5)
                    )
                    |
                    F.col("m_score").isNull()
                    |
                    (
                        ~F.col(
                            "m_score"
                        ).between(1, 5)
                    ),
                    1,
                ).otherwise(0)
            ).alias(
                "invalid_score_count"
            ),

            # 综合分数不在3～15范围
            F.sum(
                F.when(
                    F.col(
                        "rfm_total_score"
                    ).isNull()
                    |
                    (
                        ~F.col(
                            "rfm_total_score"
                        ).between(3, 15)
                    ),
                    1,
                ).otherwise(0)
            ).alias(
                "invalid_total_score_count"
            ),

            # 未完成分类的用户
            F.sum(
                F.when(
                    F.col(
                        "user_segment"
                    ) == "未分类",
                    1,
                ).otherwise(0)
            ).alias(
                "unclassified_count"
            ),
        )
        .first()
    )

    # 分群汇总表合计
    summary_result = (
        summary_df
        .agg(
            F.sum(
                "user_count"
            ).alias(
                "user_count"
            ),

            F.sum(
                "total_monetary"
            ).alias(
                "total_monetary"
            ),
        )
        .first()
    )

    source_user_count = (
        source_result["user_count"]
    )

    segment_user_count = (
        segment_result["user_count"]
    )

    distinct_user_count = (
        segment_result[
            "distinct_user_count"
        ]
    )

    summary_user_count = (
        summary_result["user_count"]
    )

    source_total_monetary = (
        source_result["total_monetary"]
        or Decimal("0.00")
    )

    segment_total_monetary = (
        segment_result["total_monetary"]
        or Decimal("0.00")
    )

    summary_total_monetary = (
        summary_result["total_monetary"]
        or Decimal("0.00")
    )

    invalid_score_count = (
        segment_result[
            "invalid_score_count"
        ] or 0
    )

    invalid_total_score_count = (
        segment_result[
            "invalid_total_score_count"
        ] or 0
    )

    unclassified_count = (
        segment_result[
            "unclassified_count"
        ] or 0
    )

    segment_count = summary_df.count()

    print("\n========== RFM评分检查 ==========")
    print(f"基础用户数：{source_user_count}")
    print(f"分群用户数：{segment_user_count}")
    print(f"不重复用户数：{distinct_user_count}")
    print(f"汇总用户数：{summary_user_count}")
    print(f"用户分群数量：{segment_count}")

    print(
        "非法R/F/M评分数量："
        f"{invalid_score_count}"
    )

    print(
        "非法综合评分数量："
        f"{invalid_total_score_count}"
    )

    print(
        "未分类用户数量："
        f"{unclassified_count}"
    )

    print(
        "基础表消费金额："
        f"{source_total_monetary}"
    )

    print(
        "分群明细消费金额："
        f"{segment_total_monetary}"
    )

    print(
        "分群汇总消费金额："
        f"{summary_total_monetary}"
    )

    if (
        source_user_count
        != segment_user_count
    ):
        raise ValueError(
            "RFM基础表与分群表用户数不一致"
        )

    if (
        segment_user_count
        != distinct_user_count
    ):
        raise ValueError(
            "RFM分群表存在重复用户"
        )

    if (
        segment_user_count
        != summary_user_count
    ):
        raise ValueError(
            "RFM分群明细与汇总用户数不一致"
        )

    if invalid_score_count != 0:
        raise ValueError(
            "RFM存在1～5以外的评分"
        )

    if invalid_total_score_count != 0:
        raise ValueError(
            "RFM综合分数不在3～15范围"
        )

    if unclassified_count != 0:
        raise ValueError(
            "RFM存在未分类用户"
        )

    if (
        abs(
            source_total_monetary
            - segment_total_monetary
        )
        > Decimal("0.01")
    ):
        raise ValueError(
            "RFM基础表与分群表金额不一致"
        )

    if (
        abs(
            source_total_monetary
            - summary_total_monetary
        )
        > Decimal("0.01")
    ):
        raise ValueError(
            "RFM基础表与汇总表金额不一致"
        )

    print("RFM评分与分群检查全部通过")


# =========================================================
# 8. 通用保存函数
# =========================================================

def save_and_check(
        df,
        output_path,
        table_name,
):
    before_save_count = df.count()

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    (
        df
        .write
        .mode("overwrite")
        .parquet(
            str(output_path)
        )
    )

    saved_df = (
        df.sparkSession
        .read
        .parquet(
            str(output_path)
        )
    )

    after_save_count = (
        saved_df.count()
    )

    print(f"\n========== {table_name} ==========")
    print(f"保存前数量：{before_save_count}")
    print(f"保存后数量：{after_save_count}")
    print(f"输出目录：{output_path}")

    if (
        before_save_count
        != after_save_count
    ):
        raise ValueError(
            f"{table_name}保存前后数量不一致"
        )

    print(f"{table_name}保存成功")


# =========================================================
# 9. 主程序
# =========================================================

def main():
    spark = create_spark()

    rfm_base_df = None
    segment_df = None
    summary_df = None

    try:
        print("开始构建Spark RFM评分与分群")

        rfm_base_df = read_rfm_base(
            spark
        )

        print(
            "RFM基础用户数："
            f"{rfm_base_df.count()}"
        )

        scored_df = add_rfm_scores(
            rfm_base_df
        )

        segment_df = add_user_segment(
            scored_df
        )

        summary_df = build_segment_summary(
            segment_df
        )

        print("\n========== 分群统计 ==========")
        (
            summary_df
            .orderBy(
                F.col(
                    "total_monetary"
                ).desc()
            )
            .show(
                20,
                truncate=False,
            )
        )

        print("\n========== 重要价值用户示例 ==========")
        (
            segment_df
            .filter(
                F.col(
                    "user_segment"
                ) == "重要价值用户"
            )
            .orderBy(
                F.col("monetary").desc()
            )
            .select(
                "customer_id",
                "customer_name",
                "recency_days",
                "frequency",
                "monetary",
                "r_score",
                "f_score",
                "m_score",
                "rfm_total_score",
                "user_segment",
            )
            .show(
                10,
                truncate=False,
            )
        )

        check_segment_result(
            rfm_base_df,
            segment_df,
            summary_df,
        )

        save_and_check(
            segment_df,
            RFM_SEGMENT_OUTPUT_PATH,
            "RFM分群明细表",
        )

        save_and_check(
            summary_df,
            RFM_SUMMARY_OUTPUT_PATH,
            "RFM分群汇总表",
        )

        print("\nSpark RFM评分与分群构建完成")

    finally:
        if rfm_base_df is not None:
            rfm_base_df.unpersist()

        if segment_df is not None:
            segment_df.unpersist()

        if summary_df is not None:
            summary_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
