from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. 项目路径
# =========================================================

PROJECT_DIR = (
    Path(__file__)
    .resolve()
    .parent
    .parent
)

ORDER_DETAIL_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ods"
    / "ods_order_detail"
)

PRODUCT_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ods"
    / "ods_product"
)


# =========================================================
# 2. 创建Spark程序
# =========================================================

spark = (
    SparkSession.builder
    .appName("BroadcastJoinLearning")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 3. 暂时关闭Spark自动广播
# =========================================================

spark.conf.set(
    "spark.sql.autoBroadcastJoinThreshold",
    -1,
)

print()
print("=" * 60)
print("广播关联学习")
print("=" * 60)

print(
    "自动广播阈值：",
    spark.conf.get(
        "spark.sql.autoBroadcastJoinThreshold"
    ),
)


# =========================================================
# 4. 读取订单明细大表
# =========================================================

detail_df = (
    spark.read.parquet(
        str(ORDER_DETAIL_PATH)
    )
    .select(
        "order_detail_id",
        "order_id",
        "product_id",
        F.col("quantity")
        .cast("long")
        .alias("quantity"),

        F.col("actual_amount")
        .cast("decimal(20,2)")
        .alias("actual_amount"),
    )
)


# =========================================================
# 5. 读取商品小表
# =========================================================

product_df = (
    spark.read.parquet(
        str(PRODUCT_PATH)
    )
    .select(
        "product_id",
        "product_name",
        "category_name",
        "brand_name",
    )
)


# =========================================================
# 6. 查看两张表的数据量
# =========================================================

detail_count = detail_df.count()
product_count = product_df.count()

print()
print("订单明细行数：", detail_count)
print("商品表行数：", product_count)

print(
    "订单明细分区数：",
    detail_df.rdd.getNumPartitions(),
)

print(
    "商品表分区数：",
    product_df.rdd.getNumPartitions(),
)


# =========================================================
# 7. 普通关联
# =========================================================

normal_join_df = (
    detail_df.alias("detail")
    .join(
        product_df.alias("product"),

        F.col("detail.product_id")
        == F.col("product.product_id"),

        "left",
    )
    .select(
        F.col("detail.order_detail_id"),
        F.col("detail.order_id"),
        F.col("detail.product_id"),
        F.col("detail.quantity"),
        F.col("detail.actual_amount"),

        F.col("product.product_id")
        .alias("matched_product_id"),

        F.col("product.product_name"),
        F.col("product.category_name"),
        F.col("product.brand_name"),
    )
)


# =========================================================
# 8. 查看普通关联执行计划
# =========================================================

print()
print("=" * 60)
print("一、普通关联执行计划")
print("=" * 60)

normal_join_df.explain(
    mode="formatted"
)


# =========================================================
# 9. 执行普通关联
# =========================================================

normal_start = perf_counter()

normal_result = (
    normal_join_df
    .agg(
        F.count("*")
        .alias("total_rows"),

        F.sum(
            F.when(
                F.col("matched_product_id").isNull(),
                1,
            )
            .otherwise(0)
        )
        .alias("unmatched_rows"),
    )
    .first()
)

normal_end = perf_counter()

print()
print("普通关联结果：")
print(
    "关联后总行数：",
    normal_result["total_rows"],
)
print(
    "未匹配商品行数：",
    normal_result["unmatched_rows"],
)
print(
    "普通关联耗时：",
    round(
        normal_end - normal_start,
        4,
    ),
    "秒",
)


# =========================================================
# 10. 广播商品表后进行关联
# =========================================================

broadcast_join_df = (
    detail_df.alias("detail")
    .join(
        F.broadcast(
            product_df
        ).alias("product"),

        F.col("detail.product_id")
        == F.col("product.product_id"),

        "left",
    )
    .select(
        F.col("detail.order_detail_id"),
        F.col("detail.order_id"),
        F.col("detail.product_id"),
        F.col("detail.quantity"),
        F.col("detail.actual_amount"),

        F.col("product.product_id")
        .alias("matched_product_id"),

        F.col("product.product_name"),
        F.col("product.category_name"),
        F.col("product.brand_name"),
    )
)


# =========================================================
# 11. 查看广播关联执行计划
# =========================================================

print()
print("=" * 60)
print("二、广播关联执行计划")
print("=" * 60)

broadcast_join_df.explain(
    mode="formatted"
)


# =========================================================
# 12. 执行广播关联
# =========================================================

broadcast_start = perf_counter()

broadcast_result = (
    broadcast_join_df
    .agg(
        F.count("*")
        .alias("total_rows"),

        F.sum(
            F.when(
                F.col("matched_product_id").isNull(),
                1,
            )
            .otherwise(0)
        )
        .alias("unmatched_rows"),
    )
    .first()
)

broadcast_end = perf_counter()

print()
print("广播关联结果：")
print(
    "关联后总行数：",
    broadcast_result["total_rows"],
)
print(
    "未匹配商品行数：",
    broadcast_result["unmatched_rows"],
)
print(
    "广播关联耗时：",
    round(
        broadcast_end - broadcast_start,
        4,
    ),
    "秒",
)


# =========================================================
# 13. 核对两种关联结果
# =========================================================

print()
print("=" * 60)
print("三、关联结果核对")
print("=" * 60)

normal_total_rows = normal_result[
    "total_rows"
]

broadcast_total_rows = broadcast_result[
    "total_rows"
]

normal_unmatched_rows = normal_result[
    "unmatched_rows"
]

broadcast_unmatched_rows = broadcast_result[
    "unmatched_rows"
]

print(
    "普通关联总行数：",
    normal_total_rows,
)

print(
    "广播关联总行数：",
    broadcast_total_rows,
)

print(
    "普通关联未匹配数：",
    normal_unmatched_rows,
)

print(
    "广播关联未匹配数：",
    broadcast_unmatched_rows,
)

if (
    normal_total_rows
    == broadcast_total_rows
    == detail_count
    and normal_unmatched_rows
    == broadcast_unmatched_rows
):
    print("检查通过：两种关联结果一致")
else:
    spark.stop()

    raise ValueError(
        "普通关联与广播关联结果不一致"
    )


# =========================================================
# 14. 查看部分关联结果
# =========================================================

print()
print("=" * 60)
print("四、广播关联结果示例")
print("=" * 60)

broadcast_join_df.show(
    10,
    truncate=False,
)


# =========================================================
# 15. 停止Spark程序
# =========================================================

spark.stop()

print()
print("Spark广播关联学习完成")
