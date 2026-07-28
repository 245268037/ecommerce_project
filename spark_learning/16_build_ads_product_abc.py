from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =========================================================
# 1. 路径配置
# =========================================================

PROJECT_DIR = Path(__file__).resolve().parents[1]

DWS_PRODUCT_INPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "dws"
    / "dws_product_sales"
)

PRODUCT_OUTPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ads"
    / "ads_product_summary"
)

ABC_SUMMARY_OUTPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ads"
    / "ads_product_abc_summary"
)


# =========================================================
# 2. 创建Spark
# =========================================================

def create_spark():
    spark = (
        SparkSession.builder
        .appName("BuildAdsProductAbc")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# =========================================================
# 3. 读取DWS商品主题
# =========================================================

def read_dws_product(spark):
    print(
        "DWS商品主题读取目录："
        f"{DWS_PRODUCT_INPUT_PATH}"
    )

    dws_product_df = (
        spark.read
        .parquet(
            str(DWS_PRODUCT_INPUT_PATH)
        )
        .cache()
    )

    return dws_product_df


# =========================================================
# 4. 计算商品总销售额
# =========================================================

def get_total_sales_amount(
        dws_product_df,
):
    result = (
        dws_product_df
        .agg(
            F.sum(
                "sales_amount"
            ).alias(
                "total_sales_amount"
            )
        )
        .first()
    )

    total_sales_amount = (
        result["total_sales_amount"]
        or Decimal("0.00")
    )

    if total_sales_amount <= 0:
        raise ValueError(
            "商品总销售额必须大于0"
        )

    print(
        "商品总销售额："
        f"{total_sales_amount}"
    )

    return total_sales_amount


# =========================================================
# 5. 计算商品排名和累计销售额
# =========================================================

def build_product_ranking(
        dws_product_df,
        total_sales_amount,
):
    # 所有商品按照销售额从高到低排队
    rank_window = (
        Window
        .orderBy(
            F.col(
                "sales_amount"
            ).desc(),
            F.col(
                "product_id"
            ).asc(),
        )
    )

    # 从第一名一直累计到当前商品
    cumulative_window = (
        Window
        .orderBy(
            F.col(
                "sales_amount"
            ).desc(),
            F.col(
                "product_id"
            ).asc(),
        )
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow,
        )
    )

    product_df = (
        dws_product_df

        # 只选择ADS需要的业务字段
        .select(
            "product_id",
            "product_name",
            "category_id",
            "category_name",
            "brand_name",
            "product_standard_price",
            "cost_price",
            "product_status",
            "sales_order_count",
            "sales_detail_count",
            "sales_count",
            "sales_amount",
            "avg_sale_price",
            "first_sale_time",
            "last_sale_time",
        )

        # 商品销售额排名
        .withColumn(
            "sales_amount_rank",
            F.row_number().over(
                rank_window
            ),
        )

        # 当前商品销售额占全部销售额的比例
        .withColumn(
            "sales_amount_rate",
            F.round(
                F.col("sales_amount")
                / F.lit(
                    total_sales_amount
                ),
                8,
            ),
        )

        # 从第一名累计到当前商品
        .withColumn(
            "cumulative_sales_amount",
            F.sum(
                "sales_amount"
            ).over(
                cumulative_window
            ),
        )

        # 先计算没有四舍五入的累计比例
        .withColumn(
            "_cumulative_rate_raw",
            F.col(
                "cumulative_sales_amount"
            )
            / F.lit(
                total_sales_amount
            ),
        )

        # 对外展示的累计销售额占比
        .withColumn(
            "cumulative_sales_amount_rate",
            F.round(
                F.col(
                    "_cumulative_rate_raw"
                ),
                8,
            ),
        )

        # 根据累计占比划分ABC等级
        .withColumn(
            "abc_level",
            F.when(
                F.col(
                    "_cumulative_rate_raw"
                ) <= F.lit(0.80),
                "A类",
            )
            .when(
                F.col(
                    "_cumulative_rate_raw"
                ) <= F.lit(0.95),
                "B类",
            )
            .otherwise(
                "C类"
            ),
        )

        # 删除内部计算用的临时字段
        .drop(
            "_cumulative_rate_raw"
        )

        # 增加ADS加工信息
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

    return product_df


# =========================================================
# 6. 构建ABC等级汇总
# =========================================================

def build_abc_summary(
        product_df,
        total_sales_amount,
):
    summary_df = (
        product_df

        # 相同ABC等级放在一起
        .groupBy(
            "abc_level"
        )

        # 计算每个等级的指标
        .agg(
            F.countDistinct(
                "product_id"
            ).alias(
                "product_count"
            ),

            F.sum(
                "sales_count"
            ).alias(
                "sales_count"
            ),

            F.sum(
                "sales_amount"
            ).alias(
                "sales_amount"
            ),
        )

        # 计算每个等级贡献的销售额占比
        .withColumn(
            "sales_amount_rate",
            F.round(
                F.col("sales_amount")
                / F.lit(
                    total_sales_amount
                ),
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
# 7. 检查商品排名和ABC结果
# =========================================================

def check_product_abc(
        dws_product_df,
        product_df,
        summary_df,
        total_sales_amount,
):
    # DWS商品主题正确值
    source_result = (
        dws_product_df
        .agg(
            F.count(
                F.lit(1)
            ).alias(
                "product_count"
            ),

            F.countDistinct(
                "product_id"
            ).alias(
                "distinct_product_count"
            ),

            F.sum(
                "sales_amount"
            ).alias(
                "sales_amount"
            ),
        )
        .first()
    )

    # ADS商品排名表统计
    target_result = (
        product_df
        .agg(
            F.count(
                F.lit(1)
            ).alias(
                "product_count"
            ),

            F.countDistinct(
                "product_id"
            ).alias(
                "distinct_product_count"
            ),

            F.countDistinct(
                "sales_amount_rank"
            ).alias(
                "distinct_rank_count"
            ),

            F.min(
                "sales_amount_rank"
            ).alias(
                "min_rank"
            ),

            F.max(
                "sales_amount_rank"
            ).alias(
                "max_rank"
            ),

            F.sum(
                "sales_amount"
            ).alias(
                "sales_amount"
            ),

            F.sum(
                F.when(
                    F.col("abc_level").isin(
                        "A类",
                        "B类",
                        "C类",
                    ),
                    0,
                ).otherwise(1)
            ).alias(
                "invalid_abc_count"
            ),
        )
        .first()
    )

    # ABC汇总表统计
    summary_result = (
        summary_df
        .agg(
            F.sum(
                "product_count"
            ).alias(
                "product_count"
            ),

            F.sum(
                "sales_amount"
            ).alias(
                "sales_amount"
            ),
        )
        .first()
    )

    # 找到排名最后一个商品
    last_product = (
        product_df
        .orderBy(
            F.col(
                "sales_amount_rank"
            ).desc()
        )
        .select(
            "product_id",
            "sales_amount_rank",
            "cumulative_sales_amount",
            "cumulative_sales_amount_rate",
        )
        .first()
    )

    source_product_count = (
        source_result["product_count"]
    )

    source_distinct_count = (
        source_result[
            "distinct_product_count"
        ]
    )

    target_product_count = (
        target_result["product_count"]
    )

    target_distinct_count = (
        target_result[
            "distinct_product_count"
        ]
    )

    distinct_rank_count = (
        target_result[
            "distinct_rank_count"
        ]
    )

    min_rank = target_result["min_rank"]
    max_rank = target_result["max_rank"]

    invalid_abc_count = (
        target_result[
            "invalid_abc_count"
        ] or 0
    )

    source_sales_amount = (
        source_result["sales_amount"]
        or Decimal("0.00")
    )

    target_sales_amount = (
        target_result["sales_amount"]
        or Decimal("0.00")
    )

    summary_product_count = (
        summary_result["product_count"]
    )

    summary_sales_amount = (
        summary_result["sales_amount"]
        or Decimal("0.00")
    )

    last_cumulative_amount = (
        last_product[
            "cumulative_sales_amount"
        ]
    )

    last_cumulative_rate = (
        last_product[
            "cumulative_sales_amount_rate"
        ]
    )

    abc_level_count = (
        summary_df.count()
    )

    print("\n========== 商品ABC检查 ==========")
    print(f"DWS商品数量：{source_product_count}")
    print(f"DWS不重复商品数：{source_distinct_count}")
    print(f"ADS商品数量：{target_product_count}")
    print(f"ADS不重复商品数：{target_distinct_count}")

    print(f"不重复排名数量：{distinct_rank_count}")
    print(f"最小排名：{min_rank}")
    print(f"最大排名：{max_rank}")

    print(f"ABC等级数量：{abc_level_count}")
    print(f"非法ABC等级数量：{invalid_abc_count}")

    print(
        "DWS商品销售额："
        f"{source_sales_amount}"
    )

    print(
        "ADS商品销售额："
        f"{target_sales_amount}"
    )

    print(
        "ABC汇总销售额："
        f"{summary_sales_amount}"
    )

    print(
        "最后一名累计销售额："
        f"{last_cumulative_amount}"
    )

    print(
        "最后一名累计占比："
        f"{last_cumulative_rate}"
    )

    if (
        source_product_count
        != source_distinct_count
    ):
        raise ValueError(
            "DWS商品主题存在重复商品"
        )

    if (
        source_product_count
        != target_product_count
    ):
        raise ValueError(
            "DWS与ADS商品数量不一致"
        )

    if (
        target_product_count
        != target_distinct_count
    ):
        raise ValueError(
            "ADS商品排名表存在重复商品"
        )

    if (
        distinct_rank_count
        != target_product_count
    ):
        raise ValueError(
            "商品排名存在重复"
        )

    if min_rank != 1:
        raise ValueError(
            "商品最小排名不是1"
        )

    if max_rank != target_product_count:
        raise ValueError(
            "商品最大排名与商品数量不一致"
        )

    if (
        summary_product_count
        != target_product_count
    ):
        raise ValueError(
            "ABC汇总商品数不一致"
        )

    if invalid_abc_count != 0:
        raise ValueError(
            "存在非法ABC等级"
        )

    if (
        abs(
            source_sales_amount
            - target_sales_amount
        )
        > Decimal("0.01")
    ):
        raise ValueError(
            "DWS与ADS商品销售额不一致"
        )

    if (
        abs(
            source_sales_amount
            - summary_sales_amount
        )
        > Decimal("0.01")
    ):
        raise ValueError(
            "ABC汇总销售额不一致"
        )

    if (
        abs(
            total_sales_amount
            - last_cumulative_amount
        )
        > Decimal("0.01")
    ):
        raise ValueError(
            "最后累计销售额不等于总销售额"
        )

    if (
        abs(
            last_cumulative_rate
            - Decimal("1.00000000")
        )
        > Decimal("0.000001")
    ):
        raise ValueError(
            "最后累计销售额占比不等于1"
        )

    print("商品排名与ABC分类检查全部通过")


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

    dws_product_df = None
    product_df = None
    summary_df = None

    try:
        print("开始构建Spark商品排名与ABC分类")

        dws_product_df = read_dws_product(
            spark
        )

        print(
            "DWS商品数量："
            f"{dws_product_df.count()}"
        )

        total_sales_amount = (
            get_total_sales_amount(
                dws_product_df
            )
        )

        product_df = (
            build_product_ranking(
                dws_product_df,
                total_sales_amount,
            )
        )

        summary_df = (
            build_abc_summary(
                product_df,
                total_sales_amount,
            )
        )

        print("\n========== 商品销售额TOP20 ==========")
        (
            product_df
            .orderBy(
                "sales_amount_rank"
            )
            .select(
                "product_id",
                "product_name",
                "category_name",
                "sales_count",
                "sales_amount",
                "sales_amount_rate",
                "sales_amount_rank",
                "cumulative_sales_amount",
                "cumulative_sales_amount_rate",
                "abc_level",
            )
            .show(
                20,
                truncate=False,
            )
        )

        print("\n========== ABC等级汇总 ==========")
        (
            summary_df
            .orderBy(
                "abc_level"
            )
            .show(
                truncate=False
            )
        )

        check_product_abc(
            dws_product_df,
            product_df,
            summary_df,
            total_sales_amount,
        )

        save_and_check(
            product_df,
            PRODUCT_OUTPUT_PATH,
            "ADS商品排名表",
        )

        save_and_check(
            summary_df,
            ABC_SUMMARY_OUTPUT_PATH,
            "ADS商品ABC汇总表",
        )

        print("\nSpark商品ABC分析构建完成")

    finally:
        if dws_product_df is not None:
            dws_product_df.unpersist()

        if product_df is not None:
            product_df.unpersist()

        if summary_df is not None:
            summary_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
