from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. 找到项目目录和订单表
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
    .appName("PartitionComparisonLearning")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 3. 查看每个分区的数据量
# =========================================================

def show_partition_rows(df, title):
    """
    查看DataFrame每个分区中有多少行数据。
    """

    print()
    print("=" * 60)
    print(title)
    print("=" * 60)

    print(
        "DataFrame分区数：",
        df.rdd.getNumPartitions(),
    )

    partition_rows = (
        df
        .select(
            F.spark_partition_id()
            .alias("partition_id")
        )
        .groupBy("partition_id")
        .agg(
            F.count("*")
            .alias("row_count")
        )
        .orderBy("partition_id")
    )

    partition_rows.show(
        100,
        truncate=False,
    )


# =========================================================
# 4. 读取ODS订单表
# =========================================================

print()
print("开始读取ODS订单表")
print("订单路径：", ORDER_PATH)

order_df = spark.read.parquet(
    str(ORDER_PATH)
)

print(
    "原始订单行数：",
    order_df.count(),
)


# =========================================================
# 5. 筛选有效订单
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
        "receiver_province_name",
        F.col("payable_amount")
        .cast("decimal(20,2)")
        .alias("payable_amount"),
    )
)

print(
    "有效订单行数：",
    valid_order_df.count(),
)


# =========================================================
# 6. 查看原始分区
# =========================================================

show_partition_rows(
    valid_order_df,
    "一、原始DataFrame的分区情况",
)


# =========================================================
# 7. 使用repartition重新划分为8个分区
# =========================================================

repartition_df = (
    valid_order_df
    .repartition(
        8,
        "receiver_province_name",
    )
)

show_partition_rows(
    repartition_df,
    "二、repartition(8, 省份)之后的分区情况",
)


print()
print("=" * 60)
print("repartition执行计划")
print("=" * 60)

repartition_df.explain(
    mode="formatted"
)


# =========================================================
# 8. 使用coalesce把8个分区合并为2个分区
# =========================================================

coalesce_df = (
    repartition_df
    .coalesce(2)
)

show_partition_rows(
    coalesce_df,
    "三、coalesce(2)之后的分区情况",
)


print()
print("=" * 60)
print("coalesce执行计划")
print("=" * 60)

coalesce_df.explain(
    mode="formatted"
)


# =========================================================
# 9. 验证调整分区没有改变数据
# =========================================================

original_count = valid_order_df.count()
repartition_count = repartition_df.count()
coalesce_count = coalesce_df.count()

print()
print("=" * 60)
print("数据行数核对")
print("=" * 60)

print("原始有效订单数：", original_count)
print("repartition后订单数：", repartition_count)
print("coalesce后订单数：", coalesce_count)

if (
    original_count
    == repartition_count
    == coalesce_count
):
    print("检查通过：调整分区没有改变数据行数")
else:
    print("检查失败：调整分区后数据行数发生变化")
    spark.stop()
    raise ValueError(
        "调整分区后数据行数不一致"
    )


# =========================================================
# 10. 结束Spark程序
# =========================================================

spark.stop()

print()
print("Spark分区对比学习完成")
