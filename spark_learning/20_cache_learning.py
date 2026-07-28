from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. 找到项目目录和订单数据
# =========================================================

PROJECT_DIR = (
    Path(__file__)
    .resolve()
    .parent
    .parent
)

ORDER_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ods"
    / "ods_order"
)


# =========================================================
# 2. 创建Spark程序
# =========================================================

spark = (
    SparkSession.builder
    .appName("SparkCacheLearning")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 3. 读取ODS订单表
# =========================================================

print()
print("=" * 60)
print("开始读取ODS订单表")
print("=" * 60)

print("订单数据路径：", ORDER_PATH)

order_df = spark.read.parquet(
    str(ORDER_PATH)
)


# =========================================================
# 4. 筛选有效订单并选择需要的字段
# =========================================================

valid_statuses = [
    "已支付",
    "已发货",
    "已完成",
]

valid_order_df = (
    order_df
    .filter(
        F.col("order_status")
        .isin(valid_statuses)
    )
    .select(
        "order_id",
        "order_status",
        "order_channel",
        "receiver_province_name",
        F.col("payable_amount")
        .cast("decimal(20,2)")
        .alias("payable_amount"),
    )
)


# =========================================================
# 5. 不使用缓存执行第一次统计
# =========================================================

print()
print("=" * 60)
print("一、不使用缓存")
print("=" * 60)

no_cache_start = perf_counter()

no_cache_count = valid_order_df.count()

no_cache_end = perf_counter()

print("有效订单数：", no_cache_count)
print(
    "第一次统计耗时：",
    round(
        no_cache_end - no_cache_start,
        4,
    ),
    "秒",
)


# =========================================================
# 6. 告诉Spark需要缓存这份数据
# =========================================================

print()
print("=" * 60)
print("二、设置缓存")
print("=" * 60)

cached_order_df = valid_order_df.cache()

print(
    "缓存级别：",
    cached_order_df.storageLevel,
)

print(
    "当前是否已经设置缓存：",
    cached_order_df.is_cached,
)


# =========================================================
# 7. 第一次执行，真正建立缓存
# =========================================================

print()
print("=" * 60)
print("三、第一次使用缓存数据")
print("=" * 60)

first_cache_start = perf_counter()

first_cache_count = cached_order_df.count()

first_cache_end = perf_counter()

print("有效订单数：", first_cache_count)
print(
    "第一次缓存计算耗时：",
    round(
        first_cache_end - first_cache_start,
        4,
    ),
    "秒",
)

print(
    "说明：第一次仍然需要读取和计算，"
    "同时把结果放入缓存。"
)


# =========================================================
# 8. 第二次使用已经建立的缓存
# =========================================================

print()
print("=" * 60)
print("四、第二次使用缓存数据")
print("=" * 60)

second_cache_start = perf_counter()

second_cache_count = cached_order_df.count()

second_cache_end = perf_counter()

print("有效订单数：", second_cache_count)
print(
    "第二次缓存计算耗时：",
    round(
        second_cache_end - second_cache_start,
        4,
    ),
    "秒",
)

print(
    "说明：第二次可以直接读取缓存，"
    "不需要重新读取Parquet并过滤。"
)


# =========================================================
# 9. 使用缓存数据统计渠道指标
# =========================================================

print()
print("=" * 60)
print("五、使用缓存统计渠道销售")
print("=" * 60)

channel_summary_df = (
    cached_order_df
    .groupBy("order_channel")
    .agg(
        F.count("*")
        .alias("order_count"),

        F.sum("payable_amount")
        .alias("sales_amount"),
    )
    .orderBy(
        F.desc("sales_amount")
    )
)

channel_summary_df.show(
    100,
    truncate=False,
)


# =========================================================
# 10. 查看缓存后的执行计划
# =========================================================

print()
print("=" * 60)
print("六、缓存后的执行计划")
print("=" * 60)

channel_summary_df.explain(
    mode="formatted"
)


# =========================================================
# 11. 核对数据行数
# =========================================================

print()
print("=" * 60)
print("七、数据行数核对")
print("=" * 60)

print("未缓存订单数：", no_cache_count)
print("第一次缓存订单数：", first_cache_count)
print("第二次缓存订单数：", second_cache_count)

if (
    no_cache_count
    == first_cache_count
    == second_cache_count
):
    print("检查通过：使用缓存没有改变数据")
else:
    cached_order_df.unpersist(
        blocking=True
    )

    spark.stop()

    raise ValueError(
        "使用缓存前后的订单数不一致"
    )


# =========================================================
# 12. 释放缓存
# =========================================================

print()
print("=" * 60)
print("八、释放缓存")
print("=" * 60)

cached_order_df.unpersist(
    blocking=True
)

print(
    "释放后是否还设置缓存：",
    cached_order_df.is_cached,
)


# =========================================================
# 13. 停止Spark程序
# =========================================================

spark.stop()

print()
print("Spark缓存学习完成")
