from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. 配置路径和业务口径
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
    / "dws_user_sales"
)


# 哪些订单状态算有效销售
VALID_SALES_STATUSES = [
    "已支付",
    "已发货",
    "已完成",
]


# =========================================================
# 2. 创建Spark运行环境
# =========================================================

def create_spark():
    spark = (
        SparkSession.builder
        .appName("BuildDwsUserSales")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# =========================================================
# 3. 读取DWD数据
# =========================================================

def read_dwd(spark):
    print(f"DWD读取目录：{DWD_INPUT_PATH}")

    dwd_df = spark.read.parquet(
        str(DWD_INPUT_PATH)
    )

    return dwd_df


# =========================================================
# 4. 恢复订单颗粒度
# =========================================================

def build_valid_order_df(dwd_df):
    # 统计全部订单数量
    all_order_count = (
        dwd_df
        .select("order_id")
        .distinct()
        .count()
    )

    # 先保留有效销售状态
    valid_detail_df = (
        dwd_df
        .filter(
            F.col("order_status").isin(
                VALID_SALES_STATUSES
            )
        )
    )

    # 只选择用户销售主题需要的字段
    valid_order_df = (
        valid_detail_df
        .select(
            "order_id",
            "customer_id",
            "customer_name",
            "gender",
            "age",
            "member_level",
            "customer_province_code",
            "customer_province_name",
            "order_time",
            "payable_amount",
        )

        # 同一个订单只保留一行
        .dropDuplicates(
            ["order_id"]
        )

        # 暂时保存计算结果，避免反复重新计算
        .cache()
    )

    valid_order_count = (
        valid_order_df.count()
    )

    excluded_order_count = (
        all_order_count
        - valid_order_count
    )

    print("\n========== 有效销售口径 ==========")
    print(f"全部订单数：{all_order_count}")
    print(f"有效订单数：{valid_order_count}")
    print(f"排除订单数：{excluded_order_count}")
    print(
        f"有效订单状态：{VALID_SALES_STATUSES}"
    )

    return valid_order_df


# =========================================================
# 5. 按用户汇总
# =========================================================

def build_user_sales(valid_order_df):
    dws_user_df = (
        valid_order_df

        # 把属于同一个用户的订单放到一起
        .groupBy(
            "customer_id",
            "customer_name",
            "gender",
            "age",
            "member_level",
            "customer_province_code",
            "customer_province_name",
        )

        # 对每个用户进行统计
        .agg(
            F.countDistinct(
                "order_id"
            ).alias(
                "order_count"
            ),

            F.sum(
                "payable_amount"
            ).alias(
                "total_amount"
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

        # 计算客单价
        .withColumn(
            "avg_order_amount",
            F.round(
                F.col("total_amount")
                / F.col("order_count"),
                2,
            )
        )

        # 增加ETL加工时间
        .withColumn(
            "etl_time",
            F.current_timestamp(),
        )

        # 记录数据来源
        .withColumn(
            "source_system",
            F.lit("ecommerce"),
        )

        # 生成本次任务批次号
        .withColumn(
            "etl_batch",
            F.date_format(
                F.current_timestamp(),
                "yyyyMMddHHmmss",
            ),
        )

        .cache()
    )

    return dws_user_df


# =========================================================
# 6. 跨层指标核对
# =========================================================

def check_user_sales(
        valid_order_df,
        dws_user_df,
):
    # DWD订单层的正确结果
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
                "total_amount"
            ),
        )
        .first()
    )

    # DWS用户主题汇总后的结果
    target_result = (
        dws_user_df
        .agg(
            F.sum(
                "order_count"
            ).alias(
                "order_count"
            ),

            F.sum(
                "total_amount"
            ).alias(
                "total_amount"
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

    # 检查一个用户是否被分成了多行
    duplicate_user_count = (
        dws_user_df
        .groupBy("customer_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    user_count = dws_user_df.count()

    print("\n========== DWS用户主题核对 ==========")
    print(f"DWS用户数：{user_count}")

    print(
        "DWD有效订单数："
        f"{source_order_count}"
    )

    print(
        "DWS订单数合计："
        f"{target_order_count}"
    )

    print(
        "DWD有效订单金额："
        f"{source_total_amount}"
    )

    print(
        "DWS用户金额合计："
        f"{target_total_amount}"
    )

    print(
        "金额差异："
        f"{amount_difference}"
    )

    print(
        "重复用户数量："
        f"{duplicate_user_count}"
    )

    if (
        source_order_count
        != target_order_count
    ):
        raise ValueError(
            "DWD订单数与DWS订单数不一致"
        )

    if amount_difference > Decimal("0.01"):
        raise ValueError(
            "DWD订单金额与DWS用户金额不一致"
        )

    if duplicate_user_count != 0:
        raise ValueError(
            "DWS用户主题存在重复用户"
        )

    print("DWS用户主题指标核对通过")


# =========================================================
# 7. 保存并回读
# =========================================================

def save_and_check(dws_user_df):
    before_save_count = (
        dws_user_df.count()
    )

    (
        dws_user_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(
            str(DWS_OUTPUT_PATH)
        )
    )

    saved_df = (
        dws_user_df.sparkSession
        .read
        .parquet(
            str(DWS_OUTPUT_PATH)
        )
    )

    after_save_count = (
        saved_df.count()
    )
    print("\n========== DWS保存检查 ==========")
    print(f"保存前用户数：{before_save_count}")
    print(f"保存后用户数：{after_save_count}")
    print(f"输出目录：{DWS_OUTPUT_PATH}")

    if (
        before_save_count
        != after_save_count
    ):
        raise ValueError(
            "DWS保存前后用户数量不一致"
        )

    print("DWS用户销售主题保存成功")


# =========================================================
# 8. 主程序
# =========================================================

def main():
    spark = create_spark()

    valid_order_df = None
    dws_user_df = None

    try:
        print("开始构建Spark DWS用户销售主题")

        dwd_df = read_dwd(spark)

        print("\n========== DWD基本信息 ==========")
        print(f"DWD明细行数：{dwd_df.count()}")

        valid_order_df = build_valid_order_df(
            dwd_df
        )

        dws_user_df = build_user_sales(
            valid_order_df
        )

        print("\n========== DWS字段结构 ==========")
        dws_user_df.printSchema()

        print("\n========== DWS示例数据 ==========")
        (
            dws_user_df
            .orderBy(
                F.col(
                    "total_amount"
                ).desc()
            )
            .show(
                10,
                truncate=False,
            )
        )

        check_user_sales(
            valid_order_df,
            dws_user_df,
        )

        save_and_check(
            dws_user_df
        )

        print("\nSpark DWS用户主题构建完成")

    finally:
        # 删除暂存在内存里的计算结果
        if valid_order_df is not None:
            valid_order_df.unpersist()

        if dws_user_df is not None:
            dws_user_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
