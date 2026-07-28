from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. 项目路径和业务配置
# =========================================================

PROJECT_DIR = Path(__file__).resolve().parents[1]

DWD_INPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "dwd"
    / "dwd_order_detail"
)

DWS_OUTPUT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "dws"
    / "dws_product_sales"
)


# 有效销售订单状态
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
        .appName("BuildDwsProductSales")
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
# 4. 筛选有效商品销售明细
# =========================================================

def build_valid_product_detail(dwd_df):
    all_detail_count = (
        dwd_df.count()
    )

    valid_detail_df = (
        dwd_df

        # 只保留有效销售订单
        .filter(
            F.col("order_status").isin(
                VALID_SALES_STATUSES
            )
        )

        # 只拿商品主题需要的字段
        .select(
            "order_detail_id",
            "order_id",
            "order_time",
            "product_id",
            "product_name",
            "category_id",
            "category_name",
            "brand_name",
            "product_standard_price",
            "cost_price",
            "product_status",
            "quantity",
            "actual_amount",
        )

        # 暂存结果，后面会多次使用
        .cache()
    )

    valid_detail_count = (
        valid_detail_df.count()
    )

    excluded_detail_count = (
        all_detail_count
        - valid_detail_count
    )

    print("\n========== 商品销售口径 ==========")
    print(f"全部商品明细数：{all_detail_count}")
    print(f"有效商品明细数：{valid_detail_count}")
    print(f"排除商品明细数：{excluded_detail_count}")
    print(f"有效状态：{VALID_SALES_STATUSES}")

    return valid_detail_df


# =========================================================
# 5. 按商品汇总
# =========================================================

def build_product_sales(
        valid_detail_df,
):
    dws_product_df = (
        valid_detail_df

        # 把相同商品的销售明细放到一起
        .groupBy(
            "product_id",
            "product_name",
            "category_id",
            "category_name",
            "brand_name",
            "product_standard_price",
            "cost_price",
            "product_status",
        )

        # 对每个商品进行统计
        .agg(
            # 有多少个订单购买过这个商品
            F.countDistinct(
                "order_id"
            ).alias(
                "sales_order_count"
            ),

            # 这个商品有多少条销售明细
            F.countDistinct(
                "order_detail_id"
            ).alias(
                "sales_detail_count"
            ),

            # 商品总销量
            F.sum(
                "quantity"
            ).alias(
                "sales_count"
            ),

            # 商品实际销售金额
            F.sum(
                "actual_amount"
            ).alias(
                "sales_amount"
            ),

            # 第一次销售时间
            F.min(
                "order_time"
            ).alias(
                "first_sale_time"
            ),

            # 最近一次销售时间
            F.max(
                "order_time"
            ).alias(
                "last_sale_time"
            ),
        )

        # 平均每件商品的实际售价
        .withColumn(
            "avg_sale_price",
            F.when(
                F.col("sales_count") > 0,
                F.round(
                    F.col("sales_amount")
                    / F.col("sales_count"),
                    2,
                ),
            ).otherwise(
                F.lit(0)
            ),
        )

        # 增加ETL时间
        .withColumn(
            "etl_time",
            F.current_timestamp(),
        )

        # 标记数据来源
        .withColumn(
            "source_system",
            F.lit("ecommerce"),
        )

        # 生成本次加工批次
        .withColumn(
            "etl_batch",
            F.date_format(
                F.current_timestamp(),
                "yyyyMMddHHmmss",
            ),
        )

        .cache()
    )

    return dws_product_df


# =========================================================
# 6. 执行跨层指标核对
# =========================================================

def check_product_sales(
        valid_detail_df,
        dws_product_df,
):
    # 统计DWD有效商品明细
    source_result = (
        valid_detail_df
        .agg(
            F.countDistinct(
                "order_detail_id"
            ).alias(
                "detail_count"
            ),

            F.sum(
                "quantity"
            ).alias(
                "sales_count"
            ),

            F.sum(
                "actual_amount"
            ).alias(
                "sales_amount"
            ),
        )
        .first()
    )

    # 统计DWS商品主题合计
    target_result = (
        dws_product_df
        .agg(
            F.sum(
                "sales_detail_count"
            ).alias(
                "detail_count"
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
        .first()
    )

    source_detail_count = (
        source_result["detail_count"]
    )

    target_detail_count = (
        target_result["detail_count"]
    )

    source_sales_count = (
        source_result["sales_count"] or 0
    )

    target_sales_count = (
        target_result["sales_count"] or 0
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

    product_count = (
        dws_product_df.count()
    )

    duplicate_product_count = (
        dws_product_df
        .groupBy("product_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    print("\n========== 商品主题指标核对 ==========")
    print(f"DWS商品数：{product_count}")

    print(
        "DWD有效商品明细数："
        f"{source_detail_count}"
    )

    print(
        "DWS商品明细数合计："
        f"{target_detail_count}"
    )

    print(
        "DWD有效商品销量："
        f"{source_sales_count}"
    )

    print(
        "DWS商品销量合计："
        f"{target_sales_count}"
    )

    print(
        "DWD有效商品金额："
        f"{source_sales_amount}"
    )

    print(
        "DWS商品金额合计："
        f"{target_sales_amount}"
    )

    print(
        "商品金额差异："
        f"{amount_difference}"
    )

    print(
        "重复商品数量："
        f"{duplicate_product_count}"
    )

    if (
        source_detail_count
        != target_detail_count
    ):
        raise ValueError(
            "DWD明细数与DWS商品明细数不一致"
        )

    if (
        source_sales_count
        != target_sales_count
    ):
        raise ValueError(
            "DWD商品销量与DWS商品销量不一致"
        )

    if (
        amount_difference
        > Decimal("0.01")
    ):
        raise ValueError(
            "DWD商品金额与DWS商品金额不一致"
        )

    if duplicate_product_count != 0:
        raise ValueError(
            "DWS商品主题存在重复商品"
        )

    print("DWS商品主题指标核对通过")


# =========================================================
# 7. 保存并回读
# =========================================================

def save_and_check(dws_product_df):
    before_save_count = (
        dws_product_df.count()
    )

    DWS_OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    (
        dws_product_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(
            str(DWS_OUTPUT_PATH)
        )
    )

    saved_df = (
        dws_product_df.sparkSession
        .read
        .parquet(
            str(DWS_OUTPUT_PATH)
        )
    )

    after_save_count = (
        saved_df.count()
    )
    print("\n========== 保存结果检查 ==========")
    print(f"保存前商品数：{before_save_count}")
    print(f"保存后商品数：{after_save_count}")
    print(f"输出目录：{DWS_OUTPUT_PATH}")

    if (
        before_save_count
        != after_save_count
    ):
        raise ValueError(
            "DWS商品主题保存前后数量不一致"
        )

    print("DWS商品销售主题保存成功")


# =========================================================
# 8. 主程序
# =========================================================

def main():
    spark = create_spark()

    valid_detail_df = None
    dws_product_df = None

    try:
        print("开始构建Spark DWS商品销售主题")

        dwd_df = read_dwd(spark)

        print("\n========== DWD基本信息 ==========")
        print(f"DWD明细行数：{dwd_df.count()}")

        valid_detail_df = (
            build_valid_product_detail(
                dwd_df
            )
        )

        dws_product_df = (
            build_product_sales(
                valid_detail_df
            )
        )

        print("\n========== DWS字段结构 ==========")
        dws_product_df.printSchema()

        print("\n========== 销售额最高的商品 ==========")
        (
            dws_product_df
            .orderBy(
                F.col(
                    "sales_amount"
                ).desc()
            )
            .show(
                10,
                truncate=False,
            )
        )

        check_product_sales(
            valid_detail_df,
            dws_product_df,
        )

        save_and_check(
            dws_product_df
        )

        print("\nSpark DWS商品主题构建完成")

    finally:
        if valid_detail_df is not None:
            valid_detail_df.unpersist()

        if dws_product_df is not None:
            dws_product_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
