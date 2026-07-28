import re
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import Window
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
    / "upsert_order_detail"
)

CORRECTION_COUNT = 100


# =========================================================
# 2. 读取并验证月份参数
# =========================================================

if len(sys.argv) != 2:
    print(
        "使用方法："
        "spark-submit "
        "27_incremental_upsert_learning.py "
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
        f"IncrementalUpsert_{target_month}"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 4. 检查并读取DWD数据
# =========================================================

if not SOURCE_PATH.exists():
    spark.stop()

    raise FileNotFoundError(
        f"源数据目录不存在：{SOURCE_PATH}"
    )


source_df = (
    spark.read.parquet(
        str(SOURCE_PATH)
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
# 5. 取得目标月份的原始数据
# =========================================================

base_month_df = (
    source_df
    .filter(
        F.col("order_month")
        == target_month
    )
)

base_count = (
    base_month_df.count()
)

base_distinct_count = (
    base_month_df
    .select("order_detail_id")
    .distinct()
    .count()
)

base_duplicate_count = (
    base_count
    - base_distinct_count
)


print()
print("=" * 70)
print("目标月份原始数据")
print("=" * 70)

print("处理月份：", target_month)
print("原始数据行数：", base_count)

print(
    "明细编号去重后数量：",
    base_distinct_count,
)

print(
    "原始重复明细数量：",
    base_duplicate_count,
)


if base_count == 0:
    spark.stop()

    raise ValueError(
        f"目标月份没有数据：{target_month}"
    )


if base_duplicate_count > 0:
    spark.stop()

    raise ValueError(
        "原始数据已经存在重复明细编号，"
        "请先处理原始重复数据"
    )


# =========================================================
# 6. 给原始数据增加版本信息
# =========================================================

base_version_df = (
    base_month_df
    .withColumn(
        "record_update_time",
        F.to_timestamp(
            F.lit(
                "2025-01-01 00:00:00"
            )
        ),
    )
    .withColumn(
        "record_source",
        F.lit("ORIGINAL"),
    )
)


# =========================================================
# 7. 模拟100条新的修正数据
# =========================================================

incremental_df = (
    base_version_df
    .orderBy("order_detail_id")
    .limit(CORRECTION_COUNT)
    .withColumn(
        "record_update_time",
        F.current_timestamp(),
    )
    .withColumn(
        "record_source",
        F.lit("CORRECTED"),
    )
    .cache()
)

incremental_count = (
    incremental_df.count()
)


print()
print("=" * 70)
print("模拟增量修正数据")
print("=" * 70)

print(
    "计划生成修正数据量：",
    CORRECTION_COUNT,
)

print(
    "实际生成修正数据量：",
    incremental_count,
)

incremental_df.select(
    "order_detail_id",
    "order_month",
    "record_source",
    "record_update_time",
).show(
    10,
    truncate=False,
)


# =========================================================
# 8. 合并原始数据和增量数据
# =========================================================

combined_df = (
    base_version_df
    .unionByName(
        incremental_df
    )
)

combined_count = (
    combined_df.count()
)


print()
print("=" * 70)
print("合并原始数据和增量数据")
print("=" * 70)

print("原始数据量：", base_count)
print("增量数据量：", incremental_count)
print("合并后的数据量：", combined_count)


# =========================================================
# 9. 检查合并后有多少重复编号
# =========================================================

duplicate_key_count = (
    combined_df
    .groupBy("order_detail_id")
    .agg(
        F.count("*")
        .alias("record_count")
    )
    .filter(
        F.col("record_count") > 1
    )
    .count()
)

print(
    "合并后重复明细编号数量：",
    duplicate_key_count,
)


# =========================================================
# 10. 设置保留最新版本的排序规则
# =========================================================

latest_record_window = (
    Window
    .partitionBy(
        "order_detail_id"
    )
    .orderBy(
        F.col("record_update_time")
        .desc(),

        F.col("record_source")
        .desc(),
    )
)


# =========================================================
# 11. 给每个明细编号的版本排序
# =========================================================

ranked_df = (
    combined_df
    .withColumn(
        "version_number",
        F.row_number()
        .over(
            latest_record_window
        ),
    )
)


# =========================================================
# 12. 每个明细编号只保留最新版本
# =========================================================

merged_df = (
    ranked_df
    .filter(
        F.col("version_number") == 1
    )
    .drop("version_number")
    .cache()
)


# =========================================================
# 13. 检查合并后的最终结果
# =========================================================

merged_count = (
    merged_df.count()
)

merged_distinct_count = (
    merged_df
    .select("order_detail_id")
    .distinct()
    .count()
)

remaining_duplicate_count = (
    merged_count
    - merged_distinct_count
)

corrected_kept_count = (
    merged_df
    .filter(
        F.col("record_source")
        == "CORRECTED"
    )
    .count()
)


print()
print("=" * 70)
print("保留最新版本后的结果")
print("=" * 70)

print("最终数据行数：", merged_count)

print(
    "最终明细编号去重数：",
    merged_distinct_count,
)

print(
    "最终剩余重复数量：",
    remaining_duplicate_count,
)

print(
    "最终保留修正版数量：",
    corrected_kept_count,
)


# =========================================================
# 14. 展示修正版被保留下来的结果
# =========================================================

print()
print("=" * 70)
print("最终保留的修正版示例")
print("=" * 70)

(
    merged_df
    .filter(
        F.col("record_source")
        == "CORRECTED"
    )
    .select(
        "order_detail_id",
        "order_id",
        "order_month",
        "record_source",
        "record_update_time",
    )
    .orderBy("order_detail_id")
    .show(
        20,
        truncate=False,
    )
)


# =========================================================
# 15. 最终业务检查
# =========================================================

expected_combined_count = (
    base_count
    + incremental_count
)

checks_passed = (
    combined_count
    == expected_combined_count
    and duplicate_key_count
    == incremental_count
    and merged_count
    == base_count
    and merged_distinct_count
    == merged_count
    and remaining_duplicate_count
    == 0
    and corrected_kept_count
    == incremental_count
)


if not checks_passed:
    incremental_df.unpersist(
        blocking=True
    )

    merged_df.unpersist(
        blocking=True
    )

    spark.stop()

    raise ValueError(
        "增量合并结果检查失败"
    )


print()
print("检查通过：每个明细编号只保留最新版本")


# =========================================================
# 16. 保存合并后的目标月份
# =========================================================

(
    merged_df
    .repartition(2)
    .write
    .mode("overwrite")
    .partitionBy("order_month")
    .parquet(
        str(OUTPUT_PATH)
    )
)


# =========================================================
# 17. 重新读取输出结果
# =========================================================

output_df = spark.read.parquet(
    str(OUTPUT_PATH)
)

output_count = (
    output_df.count()
)

output_duplicate_count = (
    output_count
    - output_df
    .select("order_detail_id")
    .distinct()
    .count()
)


print()
print("=" * 70)
print("输出文件最终检查")
print("=" * 70)

print("重新读取行数：", output_count)

print(
    "重新读取重复数量：",
    output_duplicate_count,
)


if (
    output_count != base_count
    or output_duplicate_count != 0
):
    incremental_df.unpersist(
        blocking=True
    )

    merged_df.unpersist(
        blocking=True
    )

    spark.stop()

    raise ValueError(
        "输出文件检查失败"
    )


# =========================================================
# 18. 释放缓存并结束任务
# =========================================================

incremental_df.unpersist(
    blocking=True
)

merged_df.unpersist(
    blocking=True
)

spark.stop()

print()
print(
    f"增量合并任务执行成功：{target_month}"
)
