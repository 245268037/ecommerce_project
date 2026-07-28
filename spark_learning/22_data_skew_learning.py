from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. 基础配置
# =========================================================

PROJECT_DIR = (
    Path(__file__)
    .resolve()
    .parent
    .parent
)

TOTAL_ROWS = 100000
HOT_ROWS = 90000
PARTITION_COUNT = 8
SALT_BUCKETS = 8


# =========================================================
# 2. 创建Spark程序
# =========================================================

spark = (
    SparkSession.builder
    .appName("SparkDataSkewLearning")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 为了让本次实验中的分区数量更加直观，
# 暂时关闭自适应分区合并
spark.conf.set(
    "spark.sql.adaptive.enabled",
    "false",
)


# =========================================================
# 3. 查看各分区数据量的函数
# =========================================================

def show_partition_rows(df, title):
    """
    统计DataFrame每个分区中有多少行数据。
    """

    print()
    print("=" * 70)
    print(title)
    print("=" * 70)

    print(
        "DataFrame分区数：",
        df.rdd.getNumPartitions(),
    )

    partition_rows_df = (
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

    partition_rows_df.show(
        100,
        truncate=False,
    )


# =========================================================
# 4. 创建一份故意倾斜的数据
# =========================================================

print()
print("=" * 70)
print("创建模拟数据")
print("=" * 70)

skew_df = (
    spark.range(
        0,
        TOTAL_ROWS,
        1,
        PARTITION_COUNT,
    )
    .withColumn(
        "business_key",

        F.when(
            F.col("id") < HOT_ROWS,
            F.lit("热门商品"),
        )
        .otherwise(
            F.concat(
                F.lit("普通商品_"),
                F.pmod(
                    F.col("id"),
                    F.lit(100),
                ),
            )
        ),
    )
    .withColumn(
        "amount",
        F.lit(1),
    )
)

print(
    "模拟数据总行数：",
    skew_df.count(),
)

print(
    "热门商品数据行数：",
    skew_df
    .filter(
        F.col("business_key")
        == "热门商品"
    )
    .count(),
)


# =========================================================
# 5. 查看业务字段的数据分布
# =========================================================

print()
print("=" * 70)
print("业务字段出现次数前10名")
print("=" * 70)

(
    skew_df
    .groupBy("business_key")
    .agg(
        F.count("*")
        .alias("row_count")
    )
    .orderBy(
        F.desc("row_count")
    )
    .show(
        10,
        truncate=False,
    )
)


# =========================================================
# 6. 按业务字段重新分区
# =========================================================

skew_partition_df = (
    skew_df
    .repartition(
        PARTITION_COUNT,
        "business_key",
    )
)

show_partition_rows(
    skew_partition_df,
    "一、按照business_key分区后的数据分布",
)


# =========================================================
# 7. 普通分组汇总
# =========================================================

print()
print("=" * 70)
print("普通分组汇总")
print("=" * 70)

normal_start = perf_counter()

normal_summary_df = (
    skew_df
    .groupBy("business_key")
    .agg(
        F.count("*")
        .alias("row_count"),

        F.sum("amount")
        .alias("total_amount"),
    )
)

normal_result = (
    normal_summary_df
    .filter(
        F.col("business_key")
        == "热门商品"
    )
    .first()
)

normal_end = perf_counter()

print(
    "普通汇总热门商品行数：",
    normal_result["row_count"],
)

print(
    "普通汇总热门商品金额：",
    normal_result["total_amount"],
)

print(
    "普通汇总耗时：",
    round(
        normal_end - normal_start,
        4,
    ),
    "秒",
)


# =========================================================
# 8. 给热门数据增加盐值
# =========================================================

salted_df = (
    skew_df
    .withColumn(
        "salt",

        F.when(
            F.col("business_key")
            == "热门商品",

            F.pmod(
                F.hash(
                    F.col("id")
                ),
                F.lit(SALT_BUCKETS),
            ),
        )
        .otherwise(
            F.lit(0)
        ),
    )
)

print()
print("=" * 70)
print("热门商品盐值分布")
print("=" * 70)

(
    salted_df
    .filter(
        F.col("business_key")
        == "热门商品"
    )
    .groupBy("salt")
    .agg(
        F.count("*")
        .alias("row_count")
    )
    .orderBy("salt")
    .show(
        100,
        truncate=False,
    )
)


# =========================================================
# 9. 按照业务字段和盐值重新分区
# =========================================================

salted_partition_df = (
    salted_df
    .repartition(
        PARTITION_COUNT,
        "business_key",
        "salt",
    )
)

show_partition_rows(
    salted_partition_df,
    "二、增加盐值之后的数据分布",
)


# =========================================================
# 10. 第一次汇总：业务字段加盐值
# =========================================================

partial_summary_df = (
    salted_df
    .groupBy(
        "business_key",
        "salt",
    )
    .agg(
        F.count("*")
        .alias("partial_row_count"),

        F.sum("amount")
        .alias("partial_amount"),
    )
)


# =========================================================
# 11. 第二次汇总：去掉盐值，恢复业务结果
# =========================================================

salt_start = perf_counter()

final_summary_df = (
    partial_summary_df
    .groupBy("business_key")
    .agg(
        F.sum("partial_row_count")
        .alias("row_count"),

        F.sum("partial_amount")
        .alias("total_amount"),
    )
)

salt_result = (
    final_summary_df
    .filter(
        F.col("business_key")
        == "热门商品"
    )
    .first()
)

salt_end = perf_counter()

print()
print("=" * 70)
print("加盐后的两阶段汇总结果")
print("=" * 70)

print(
    "加盐汇总热门商品行数：",
    salt_result["row_count"],
)

print(
    "加盐汇总热门商品金额：",
    salt_result["total_amount"],
)

print(
    "加盐汇总耗时：",
    round(
        salt_end - salt_start,
        4,
    ),
    "秒",
)


# =========================================================
# 12. 核对普通汇总与加盐汇总
# =========================================================

print()
print("=" * 70)
print("结果核对")
print("=" * 70)

if (
    normal_result["row_count"]
    == salt_result["row_count"]
    == HOT_ROWS
    and normal_result["total_amount"]
    == salt_result["total_amount"]
    == HOT_ROWS
):
    print("检查通过：普通汇总与加盐汇总结果一致")
else:
    spark.stop()

    raise ValueError(
        "加盐前后的汇总结果不一致"
    )


# =========================================================
# 13. 查看加盐汇总的执行计划
# =========================================================

print()
print("=" * 70)
print("加盐两阶段汇总执行计划")
print("=" * 70)

final_summary_df.explain(
    mode="formatted"
)


# =========================================================
# 14. 停止Spark程序
# =========================================================

spark.stop()

print()
print("Spark数据倾斜学习完成")
