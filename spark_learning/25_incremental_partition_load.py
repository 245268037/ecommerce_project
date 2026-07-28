from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================
# 1. 项目路径和参数
# =========================================================

PROJECT_DIR = (
    Path(__file__)
    .resolve()
    .parent
    .parent
)

SOURCE_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
    / "dwd"
    / "dwd_order_detail"
)

OUTPUT_PATH = (
    PROJECT_DIR
    / "output"
    / "spark_learning"
    / "incremental_order_detail"
)

TARGET_MONTH = "2025-01"


# =========================================================
# 2. 创建Spark程序
# =========================================================

spark = (
    SparkSession.builder
    .appName("IncrementalPartitionLoadLearning")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 3. 开启动态分区覆盖
# =========================================================

spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic",
)

print()
print("=" * 70)
print("Spark增量分区加载学习")
print("=" * 70)

print(
    "分区覆盖模式：",
    spark.conf.get(
        "spark.sql.sources.partitionOverwriteMode"
    ),
)

print("目标重跑月份：", TARGET_MONTH)


# =========================================================
# 4. 获取每个月份文件名的函数
# =========================================================

def get_partition_files(base_path):
    """
    获取每个月份目录中的Parquet文件名称。
    """

    result = {}

    month_directories = sorted(
        directory
        for directory in base_path.glob(
            "order_month=*"
        )
        if directory.is_dir()
    )

    for directory in month_directories:
        parquet_files = sorted(
            file_path.name
            for file_path in directory.glob(
                "part-*.parquet"
            )
        )

        result[directory.name] = parquet_files

    return result


# =========================================================
# 5. 打印月份文件信息的函数
# =========================================================

def show_partition_files(file_map, title):
    """
    打印每个月份目录的Parquet文件数量。
    """

    print()
    print("=" * 70)
    print(title)
    print("=" * 70)

    for month_name, file_names in file_map.items():
        print(
            month_name,
            "文件数：",
            len(file_names),
        )


# =========================================================
# 6. 读取源DWD数据
# =========================================================

source_df = spark.read.parquet(
    str(SOURCE_PATH)
)

source_count = source_df.count()

print()
print("源数据路径：", SOURCE_PATH)
print("源数据总行数：", source_count)


# =========================================================
# 7. 生成月份字段
# =========================================================

partition_source_df = (
    source_df
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

invalid_month_count = (
    partition_source_df
    .filter(
        F.col("order_month").isNull()
    )
    .count()
)

print(
    "月份无法识别的数据量：",
    invalid_month_count,
)

valid_source_df = (
    partition_source_df
    .filter(
        F.col("order_month")
        .isNotNull()
    )
)

valid_source_count = (
    valid_source_df.count()
)

print(
    "月份有效的数据量：",
    valid_source_count,
)


# =========================================================
# 8. 第一次执行：构建完整月份数据
# =========================================================

print()
print("=" * 70)
print("第一次执行：构建完整分区数据")
print("=" * 70)

full_write_start = perf_counter()

(
    valid_source_df
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

full_write_end = perf_counter()

print(
    "完整数据写入耗时：",
    round(
        full_write_end - full_write_start,
        4,
    ),
    "秒",
)


# =========================================================
# 9. 记录增量覆盖前的文件
# =========================================================

before_file_map = get_partition_files(
    OUTPUT_PATH
)

show_partition_files(
    before_file_map,
    "增量覆盖前的月份文件",
)


# =========================================================
# 10. 读取完整输出并记录行数
# =========================================================

before_output_df = spark.read.parquet(
    str(OUTPUT_PATH)
)

before_total_count = (
    before_output_df.count()
)

before_target_count = (
    before_output_df
    .filter(
        F.col("order_month")
        == TARGET_MONTH
    )
    .count()
)

before_other_count = (
    before_output_df
    .filter(
        F.col("order_month")
        != TARGET_MONTH
    )
    .count()
)

print()
print("覆盖前总行数：", before_total_count)

print(
    "覆盖前目标月份行数：",
    before_target_count,
)

print(
    "覆盖前其他月份行数：",
    before_other_count,
)


# =========================================================
# 11. 准备目标月份增量数据
# =========================================================

incremental_df = (
    valid_source_df
    .filter(
        F.col("order_month")
        == TARGET_MONTH
    )
)

incremental_count = (
    incremental_df.count()
)

print()
print("=" * 70)
print("准备增量数据")
print("=" * 70)

print("本次处理月份：", TARGET_MONTH)

print(
    "本次增量数据行数：",
    incremental_count,
)

if incremental_count == 0:
    spark.stop()

    raise ValueError(
        f"目标月份没有数据：{TARGET_MONTH}"
    )


# =========================================================
# 12. 只覆盖目标月份
# =========================================================

print()
print("=" * 70)
print("第二次执行：动态覆盖目标月份")
print("=" * 70)

incremental_start = perf_counter()

(
    incremental_df
    .coalesce(2)
    .write
    .mode("overwrite")
    .option(
        "partitionOverwriteMode",
        "dynamic",
    )
    .partitionBy("order_month")
    .parquet(
        str(OUTPUT_PATH)
    )
)

incremental_end = perf_counter()

print(
    "目标月份覆盖耗时：",
    round(
        incremental_end - incremental_start,
        4,
    ),
    "秒",
)


# =========================================================
# 13. 记录增量覆盖后的文件
# =========================================================

after_file_map = get_partition_files(
    OUTPUT_PATH
)

show_partition_files(
    after_file_map,
    "增量覆盖后的月份文件",
)


# =========================================================
# 14. 检查目标月份文件是否被替换
# =========================================================

target_directory_name = (
    f"order_month={TARGET_MONTH}"
)

target_files_before = (
    before_file_map.get(
        target_directory_name,
        [],
    )
)

target_files_after = (
    after_file_map.get(
        target_directory_name,
        [],
    )
)

print()
print("=" * 70)
print("文件变化检查")
print("=" * 70)

print(
    "目标月份覆盖前文件：",
    target_files_before,
)

print(
    "目标月份覆盖后文件：",
    target_files_after,
)

target_files_changed = (
    target_files_before
    != target_files_after
)

print(
    "目标月份文件是否发生变化：",
    target_files_changed,
)


# =========================================================
# 15. 检查其他月份文件是否保持不变
# =========================================================

other_files_unchanged = True

for month_name, before_files in before_file_map.items():
    if month_name == target_directory_name:
        continue

    after_files = after_file_map.get(
        month_name,
        [],
    )

    if before_files != after_files:
        other_files_unchanged = False

        print(
            "其他月份文件发生变化：",
            month_name,
        )

print(
    "其他月份文件是否保持不变：",
    other_files_unchanged,
)


# =========================================================
# 16. 重新读取覆盖后的完整数据
# =========================================================

after_output_df = spark.read.parquet(
    str(OUTPUT_PATH)
)

after_total_count = (
    after_output_df.count()
)

after_target_count = (
    after_output_df
    .filter(
        F.col("order_month")
        == TARGET_MONTH
    )
    .count()
)

after_other_count = (
    after_output_df
    .filter(
        F.col("order_month")
        != TARGET_MONTH
    )
    .count()
)

print()
print("=" * 70)
print("增量覆盖后数据核对")
print("=" * 70)

print("覆盖后总行数：", after_total_count)

print(
    "覆盖后目标月份行数：",
    after_target_count,
)

print(
    "覆盖后其他月份行数：",
    after_other_count,
)


# =========================================================
# 17. 最终检查
# =========================================================

if (
    before_total_count
    == after_total_count
    == valid_source_count
    and before_target_count
    == after_target_count
    == incremental_count
    and before_other_count
    == after_other_count
    and target_files_changed
    and other_files_unchanged
):
    print()
    print("检查通过：只覆盖了目标月份")
else:
    spark.stop()

    raise ValueError(
        "动态分区覆盖检查失败"
    )


# =========================================================
# 18. 停止Spark程序
# =========================================================

spark.stop()

print()
print("Spark增量分区加载学习完成")
