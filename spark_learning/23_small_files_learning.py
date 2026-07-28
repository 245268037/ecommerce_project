from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession


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

OUTPUT_BASE_PATH = (
    PROJECT_DIR
    / "output"
    / "spark_learning"
    / "small_files"
)

DEFAULT_OUTPUT_PATH = (
    OUTPUT_BASE_PATH
    / "default_output"
)

REPARTITION_OUTPUT_PATH = (
    OUTPUT_BASE_PATH
    / "repartition_8_output"
)

COALESCE_OUTPUT_PATH = (
    OUTPUT_BASE_PATH
    / "coalesce_2_output"
)


# =========================================================
# 2. 创建Spark程序
# =========================================================

spark = (
    SparkSession.builder
    .appName("SparkSmallFilesLearning")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# 3. 统计Parquet文件信息的函数
# =========================================================

def show_output_files(output_path, title):
    """
    查看输出目录中Parquet数据文件的数量和大小。
    """

    parquet_files = sorted(
        output_path.glob(
            "part-*.parquet"
        )
    )

    total_bytes = sum(
        file_path.stat().st_size
        for file_path in parquet_files
    )

    total_mb = (
        total_bytes
        / 1024
        / 1024
    )

    print()
    print("=" * 70)
    print(title)
    print("=" * 70)

    print("输出目录：", output_path)
    print("Parquet数据文件数：", len(parquet_files))
    print("Parquet数据总大小：", round(total_mb, 2), "MB")

    for file_path in parquet_files:
        file_size_mb = (
            file_path.stat().st_size
            / 1024
            / 1024
        )

        print(
            file_path.name,
            "大小：",
            round(file_size_mb, 2),
            "MB",
        )


# =========================================================
# 4. 读取DWD订单明细
# =========================================================

print()
print("=" * 70)
print("读取DWD订单明细")
print("=" * 70)

dwd_df = spark.read.parquet(
    str(DWD_PATH)
)

source_count = dwd_df.count()

source_partitions = (
    dwd_df.rdd.getNumPartitions()
)

print("DWD数据路径：", DWD_PATH)
print("DWD数据行数：", source_count)
print("DWD原始分区数：", source_partitions)


# =========================================================
# 5. 实验一：不主动调整分区，直接保存
# =========================================================

print()
print("=" * 70)
print("实验一：按照原始分区直接保存")
print("=" * 70)

default_start = perf_counter()

(
    dwd_df
    .write
    .mode("overwrite")
    .parquet(
        str(DEFAULT_OUTPUT_PATH)
    )
)

default_end = perf_counter()

print(
    "默认保存耗时：",
    round(
        default_end - default_start,
        4,
    ),
    "秒",
)

show_output_files(
    DEFAULT_OUTPUT_PATH,
    "默认输出文件情况",
)


# =========================================================
# 6. 实验二：重新分成8个分区后保存
# =========================================================

print()
print("=" * 70)
print("实验二：repartition(8)后保存")
print("=" * 70)

repartition_df = (
    dwd_df
    .repartition(8)
)

print(
    "repartition后分区数：",
    repartition_df.rdd.getNumPartitions(),
)

repartition_start = perf_counter()

(
    repartition_df
    .write
    .mode("overwrite")
    .parquet(
        str(REPARTITION_OUTPUT_PATH)
    )
)

repartition_end = perf_counter()

print(
    "repartition保存耗时：",
    round(
        repartition_end - repartition_start,
        4,
    ),
    "秒",
)

show_output_files(
    REPARTITION_OUTPUT_PATH,
    "repartition(8)输出文件情况",
)


# =========================================================
# 7. 实验三：合并成2个分区后保存
# =========================================================

print()
print("=" * 70)
print("实验三：coalesce(2)后保存")
print("=" * 70)

coalesce_df = (
    dwd_df
    .coalesce(2)
)

print(
    "coalesce后分区数：",
    coalesce_df.rdd.getNumPartitions(),
)

coalesce_start = perf_counter()

(
    coalesce_df
    .write
    .mode("overwrite")
    .parquet(
        str(COALESCE_OUTPUT_PATH)
    )
)

coalesce_end = perf_counter()

print(
    "coalesce保存耗时：",
    round(
        coalesce_end - coalesce_start,
        4,
    ),
    "秒",
)

show_output_files(
    COALESCE_OUTPUT_PATH,
    "coalesce(2)输出文件情况",
)


# =========================================================
# 8. 重新读取三个输出目录
# =========================================================

print()
print("=" * 70)
print("重新读取并核对数据")
print("=" * 70)

default_count = (
    spark.read.parquet(
        str(DEFAULT_OUTPUT_PATH)
    )
    .count()
)

repartition_count = (
    spark.read.parquet(
        str(REPARTITION_OUTPUT_PATH)
    )
    .count()
)

coalesce_count = (
    spark.read.parquet(
        str(COALESCE_OUTPUT_PATH)
    )
    .count()
)

print("源数据行数：", source_count)
print("默认输出行数：", default_count)
print("repartition输出行数：", repartition_count)
print("coalesce输出行数：", coalesce_count)

if (
    source_count
    == default_count
    == repartition_count
    == coalesce_count
):
    print("检查通过：三种保存方式的数据行数一致")
else:
    spark.stop()

    raise ValueError(
        "不同保存方式的数据行数不一致"
    )


# =========================================================
# 9. 输出实验结论
# =========================================================

print()
print("=" * 70)
print("实验结论")
print("=" * 70)

print(
    "默认输出：按照原始DataFrame的分区数量保存"
)

print(
    "repartition(8)：重新洗牌后，"
    "通常生成8个Parquet数据文件"
)

print(
    "coalesce(2)：合并原有分区后，"
    "通常生成2个Parquet数据文件"
)


# =========================================================
# 10. 停止Spark程序
# =========================================================

spark.stop()

print()
print("Spark小文件学习完成")
