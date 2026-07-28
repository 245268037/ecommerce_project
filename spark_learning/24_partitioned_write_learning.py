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

DWD_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "dwd"
    / "dwd_order_detail"
)

OUTPUT_PATH = (
    PROJECT_DIR
    / "output"
    / "spark_learning"
    / "partitioned_dwd_order_detail"
)

TARGET_MONTH = "2025-01"


# =========================================================
# 2. 创建Spark程序
# =========================================================

spark = (
    SparkSession.builder
    .appName("PartitionedWriteLearning")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 3. 读取DWD订单明细
# =========================================================

print()
print("=" * 70)
print("读取DWD订单明细")
print("=" * 70)

dwd_df = spark.read.parquet(
    str(DWD_PATH)
)

source_count = dwd_df.count()

print("DWD路径：", DWD_PATH)
print("DWD数据行数：", source_count)
print("DWD原始分区数：", dwd_df.rdd.getNumPartitions())


# =========================================================
# 4. 根据订单时间生成日期和月份字段
# =========================================================

partition_source_df = (
    dwd_df
    .withColumn(
        "order_date",
        F.to_date(
            F.col("order_time")
        ),
    )
    .withColumn(
        "order_month",
        F.date_format(
            F.col("order_time"),
            "yyyy-MM",
        ),
    )
)


# =========================================================
# 5. 检查无法识别月份的数据
# =========================================================

invalid_month_count = (
    partition_source_df
    .filter(
        F.col("order_month").isNull()
    )
    .count()
)

print()
print(
    "无法识别订单月份的数据量：",
    invalid_month_count,
)

if invalid_month_count > 0:
    print(
        "警告：存在无法识别月份的数据，"
        "本次分区输出将排除这些数据"
    )


# =========================================================
# 6. 保留月份有效的数据
# =========================================================

valid_partition_df = (
    partition_source_df
    .filter(
        F.col("order_month")
        .isNotNull()
    )
)

valid_count = valid_partition_df.count()

print(
    "月份有效的数据量：",
    valid_count,
)


# =========================================================
# 7. 查看每个月的数据量
# =========================================================

print()
print("=" * 70)
print("各月份数据量")
print("=" * 70)

month_count_df = (
    valid_partition_df
    .groupBy("order_month")
    .agg(
        F.count("*")
        .alias("row_count")
    )
    .orderBy("order_month")
)

month_count_df.show(
    100,
    truncate=False,
)


# =========================================================
# 8. 取得目标月份的正确行数
# =========================================================

target_source_count = (
    valid_partition_df
    .filter(
        F.col("order_month")
        == TARGET_MONTH
    )
    .count()
)

print(
    "源数据中目标月份行数：",
    target_source_count,
)


# =========================================================
# 9. 按月份重新分配并保存
# =========================================================

print()
print("=" * 70)
print("按照月份分区保存")
print("=" * 70)

write_start = perf_counter()

(
    valid_partition_df
    .repartition(
        12,
        "order_month",
    )
    .write
    .mode("overwrite")
    .partitionBy("order_month")
    .parquet(
        str(OUTPUT_PATH)
    )
)

write_end = perf_counter()

print("输出路径：", OUTPUT_PATH)

print(
    "分区保存耗时：",
    round(
        write_end - write_start,
        4,
    ),
    "秒",
)


# =========================================================
# 10. 查看磁盘上的月份目录
# =========================================================

print()
print("=" * 70)
print("磁盘分区目录")
print("=" * 70)

month_directories = sorted(
    directory
    for directory in OUTPUT_PATH.glob(
        "order_month=*"
    )
    if directory.is_dir()
)

for month_directory in month_directories:
    parquet_files = sorted(
        month_directory.glob(
            "part-*.parquet"
        )
    )

    total_bytes = sum(
        file_path.stat().st_size
        for file_path in parquet_files
    )

    print(
        month_directory.name,
        "Parquet文件数：",
        len(parquet_files),
        "文件大小：",
        round(
            total_bytes / 1024 / 1024,
            2,
        ),
        "MB",
    )

print(
    "月份目录数量：",
    len(month_directories),
)


# =========================================================
# 11. 重新读取完整分区表
# =========================================================

print()
print("=" * 70)
print("重新读取完整分区表")
print("=" * 70)

partitioned_df = spark.read.parquet(
    str(OUTPUT_PATH)
)

partitioned_count = partitioned_df.count()

print(
    "重新读取后的总行数：",
    partitioned_count,
)

print()
print("重新读取后的字段结构：")

partitioned_df.printSchema()


# =========================================================
# 12. 只查询目标月份
# =========================================================

print()
print("=" * 70)
print("查询目标月份")
print("=" * 70)

target_month_df = (
    partitioned_df
    .filter(
        F.col("order_month")
        == TARGET_MONTH
    )
)

query_start = perf_counter()

target_output_count = (
    target_month_df.count()
)

query_end = perf_counter()

print("目标月份：", TARGET_MONTH)

print(
    "分区表中目标月份行数：",
    target_output_count,
)

print(
    "目标月份查询耗时：",
    round(
        query_end - query_start,
        4,
    ),
    "秒",
)


# =========================================================
# 13. 查看分区裁剪执行计划
# =========================================================

print()
print("=" * 70)
print("目标月份查询执行计划")
print("=" * 70)

target_month_df.explain(
    mode="formatted"
)


# =========================================================
# 14. 核对数据
# =========================================================

print()
print("=" * 70)
print("数据核对")
print("=" * 70)

print("源数据总行数：", source_count)
print("月份有效行数：", valid_count)
print("分区输出总行数：", partitioned_count)

print(
    "源数据目标月份行数：",
    target_source_count,
)

print(
    "分区表目标月份行数：",
    target_output_count,
)

if (
    valid_count
    == partitioned_count
    and target_source_count
    == target_output_count
):
    print("检查通过：分区保存前后数据一致")
else:
    spark.stop()

    raise ValueError(
        "分区保存前后数据不一致"
    )


# =========================================================
# 15. 停止Spark程序
# =========================================================

spark.stop()

print()
print("Spark日期分区学习完成")
