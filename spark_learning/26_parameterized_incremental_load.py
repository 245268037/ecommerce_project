import re
import sys
from datetime import datetime
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


# =========================================================
# 2. 读取并验证月份参数
# =========================================================

if len(sys.argv) != 2:
    print(
        "使用方法："
        "spark-submit "
        "26_parameterized_incremental_load.py "
        "YYYY-MM"
    )

    sys.exit(2)


target_month = sys.argv[1]


if not re.fullmatch(
    r"\d{4}-\d{2}",
    target_month,
):
    print(
        "参数错误：月份格式必须是YYYY-MM"
    )

    print(
        "正确示例：2025-01"
    )

    sys.exit(2)


try:
    datetime.strptime(
        target_month,
        "%Y-%m",
    )
except ValueError:
    print(
        f"参数错误：不是有效月份：{target_month}"
    )

    sys.exit(2)


# =========================================================
# 3. 创建Spark程序
# =========================================================

spark = (
    SparkSession.builder
    .appName(
        f"IncrementalLoad_{target_month}"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic",
)


# =========================================================
# 4. 打印任务信息
# =========================================================

print()
print("=" * 70)
print("Spark参数化增量任务开始")
print("=" * 70)

print("本次处理月份：", target_month)
print("源数据路径：", SOURCE_PATH)
print("输出数据路径：", OUTPUT_PATH)

print(
    "分区覆盖模式：",
    spark.conf.get(
        "spark.sql.sources.partitionOverwriteMode"
    ),
)


# =========================================================
# 5. 检查源数据
# =========================================================

if not SOURCE_PATH.exists():
    spark.stop()

    raise FileNotFoundError(
        f"源数据目录不存在：{SOURCE_PATH}"
    )


source_df = spark.read.parquet(
    str(SOURCE_PATH)
)


# =========================================================
# 6. 生成月份字段
# =========================================================

source_with_month_df = (
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


# =========================================================
# 7. 只保留本次目标月份
# =========================================================

incremental_df = (
    source_with_month_df
    .filter(
        F.col("order_month")
        == target_month
    )
)

incremental_count = (
    incremental_df.count()
)

print()
print(
    "本次增量数据行数：",
    incremental_count,
)

if incremental_count == 0:
    spark.stop()

    raise ValueError(
        f"目标月份没有可处理的数据：{target_month}"
    )


# =========================================================
# 8. 记录写入前的数据量
# =========================================================

before_total_count = 0
before_target_count = 0
before_other_count = 0


if OUTPUT_PATH.exists():
    before_df = spark.read.parquet(
        str(OUTPUT_PATH)
    )

    before_total_count = (
        before_df.count()
    )

    before_target_count = (
        before_df
        .filter(
            F.col("order_month")
            == target_month
        )
        .count()
    )

    before_other_count = (
        before_df
        .filter(
            F.col("order_month")
            != target_month
        )
        .count()
    )


print()
print("=" * 70)
print("写入前数据情况")
print("=" * 70)

print(
    "写入前总行数：",
    before_total_count,
)

print(
    "写入前目标月份行数：",
    before_target_count,
)

print(
    "写入前其他月份行数：",
    before_other_count,
)


# =========================================================
# 9. 动态覆盖目标月份
# =========================================================

print()
print("=" * 70)
print("开始覆盖目标月份")
print("=" * 70)

load_start = perf_counter()

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

load_end = perf_counter()

print(
    "增量写入耗时：",
    round(
        load_end - load_start,
        4,
    ),
    "秒",
)


# =========================================================
# 10. 重新读取写入后的数据
# =========================================================

after_df = spark.read.parquet(
    str(OUTPUT_PATH)
)

after_total_count = (
    after_df.count()
)

after_target_count = (
    after_df
    .filter(
        F.col("order_month")
        == target_month
    )
    .count()
)

after_other_count = (
    after_df
    .filter(
        F.col("order_month")
        != target_month
    )
    .count()
)


# =========================================================
# 11. 输出写入后的数据情况
# =========================================================

print()
print("=" * 70)
print("写入后数据情况")
print("=" * 70)

print(
    "写入后总行数：",
    after_total_count,
)

print(
    "写入后目标月份行数：",
    after_target_count,
)

print(
    "写入后其他月份行数：",
    after_other_count,
)


# =========================================================
# 12. 增量结果检查
# =========================================================

expected_total_count = (
    before_other_count
    + incremental_count
)

target_check_passed = (
    after_target_count
    == incremental_count
)

other_check_passed = (
    after_other_count
    == before_other_count
)

total_check_passed = (
    after_total_count
    == expected_total_count
)


print()
print("=" * 70)
print("增量任务检查结果")
print("=" * 70)

print(
    "目标月份检查：",
    "通过"
    if target_check_passed
    else "失败",
)

print(
    "其他月份检查：",
    "通过"
    if other_check_passed
    else "失败",
)

print(
    "总行数检查：",
    "通过"
    if total_check_passed
    else "失败",
)


if not (
    target_check_passed
    and other_check_passed
    and total_check_passed
):
    spark.stop()

    raise ValueError(
        "增量加载结果检查失败"
    )


# =========================================================
# 13. 结束任务
# =========================================================

print()
print(
    f"增量任务执行成功：{target_month}"
)

spark.stop()
