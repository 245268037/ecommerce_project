from pathlib import Path

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

WAREHOUSE_PATH = (
    PROJECT_DIR
    / "warehouse_spark"
)


# =========================================================
# 2. 需要检查的数仓表
# =========================================================

TABLE_PATHS = {
    "ODS订单表": (
        WAREHOUSE_PATH
        / "ods"
        / "ods_order"
    ),

    "ODS订单明细表": (
        WAREHOUSE_PATH
        / "ods"
        / "ods_order_detail"
    ),

    "ODS客户表": (
        WAREHOUSE_PATH
        / "ods"
        / "ods_customer"
    ),

    "ODS商品表": (
        WAREHOUSE_PATH
        / "ods"
        / "ods_product"
    ),

    "DWD订单明细表": (
        WAREHOUSE_PATH
        / "dwd"
        / "dwd_order_detail"
    ),

    "DWS用户销售主题": (
        WAREHOUSE_PATH
        / "dws"
        / "dws_user_sales"
    ),

    "DWS商品销售主题": (
        WAREHOUSE_PATH
        / "dws"
        / "dws_product_sales"
    ),

    "DWS地区销售主题": (
        WAREHOUSE_PATH
        / "dws"
        / "dws_area_sales"
    ),

    "ADS日销售指标": (
        WAREHOUSE_PATH
        / "ads"
        / "ads_sales_summary"
    ),

    "ADS用户RFM结果": (
        WAREHOUSE_PATH
        / "ads"
        / "ads_user_rfm_segment"
    ),

    "ADS商品指标": (
        WAREHOUSE_PATH
        / "ads"
        / "ads_product_summary"
    ),
}


# =========================================================
# 3. 创建Spark程序
# =========================================================

spark = (
    SparkSession.builder
    .appName(
        "SparkWarehousePerformanceAudit"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print()
print("=" * 80)
print("Spark数仓性能体检开始")
print("=" * 80)

print(
    "Spark默认并行度：",
    spark.sparkContext.defaultParallelism,
)

print(
    "Shuffle默认分区数：",
    spark.conf.get(
        "spark.sql.shuffle.partitions"
    ),
)


# =========================================================
# 4. 检查单张数仓表的函数
# =========================================================

def audit_table(table_name, table_path):
    """
    检查一张Parquet表的行数、分区数、
    数据文件数量和文件大小。
    """

    print()
    print("=" * 80)
    print(table_name)
    print("=" * 80)

    print("数据路径：", table_path)

    if not table_path.exists():
        print("检查失败：数据目录不存在")

        return None

    parquet_files = sorted(
        table_path.rglob(
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

    file_count = len(
        parquet_files
    )

    if file_count > 0:
        average_file_mb = (
            total_mb
            / file_count
        )
    else:
        average_file_mb = 0

    table_df = spark.read.parquet(
        str(table_path)
    )

    row_count = (
        table_df.count()
    )

    partition_count = (
        table_df.rdd.getNumPartitions()
    )

    print("数据行数：", row_count)
    print("计算分区数：", partition_count)
    print("Parquet数据文件数：", file_count)

    print(
        "Parquet文件总大小：",
        round(total_mb, 2),
        "MB",
    )

    print(
        "平均单文件大小：",
        round(average_file_mb, 2),
        "MB",
    )

    if (
        file_count >= 10
        and average_file_mb < 1
    ):
        print(
            "体检提示：文件数量较多且平均文件较小，"
            "存在小文件风险"
        )
    else:
        print(
            "体检提示：当前没有发现明显小文件问题"
        )

    return {
        "name": table_name,
        "path": table_path,
        "df": table_df,
        "row_count": row_count,
        "partition_count": partition_count,
        "file_count": file_count,
        "total_mb": total_mb,
        "average_file_mb": average_file_mb,
    }


# =========================================================
# 5. 检查字段分布的函数
# =========================================================

def audit_key_distribution(
    table_df,
    column_name,
    description,
):
    """
    检查一个字段是否存在明显热点值。
    """

    print()
    print("-" * 80)
    print("字段分布检查：", description)
    print("-" * 80)

    if column_name not in table_df.columns:
        print(
            f"跳过：字段不存在：{column_name}"
        )

        return None

    null_count = (
        table_df
        .filter(
            F.col(column_name).isNull()
        )
        .count()
    )

    key_count_df = (
        table_df
        .filter(
            F.col(column_name)
            .isNotNull()
        )
        .groupBy(column_name)
        .agg(
            F.count("*")
            .alias("row_count")
        )
    )

    distribution_result = (
        key_count_df
        .agg(
            F.count("*")
            .alias("key_count"),

            F.sum("row_count")
            .alias("total_rows"),

            F.max("row_count")
            .alias("max_rows"),

            F.avg("row_count")
            .alias("avg_rows"),
        )
        .first()
    )

    key_count = (
        distribution_result["key_count"]
        or 0
    )

    total_rows = (
        distribution_result["total_rows"]
        or 0
    )

    max_rows = (
        distribution_result["max_rows"]
        or 0
    )

    avg_rows = (
        distribution_result["avg_rows"]
        or 0
    )

    if avg_rows > 0:
        skew_ratio = (
            max_rows
            / avg_rows
        )
    else:
        skew_ratio = 0

    print("空值数量：", null_count)
    print("不同值数量：", key_count)
    print("非空数据总量：", total_rows)
    print("单个值最大数据量：", max_rows)

    print(
        "每个值平均数据量：",
        round(avg_rows, 2),
    )

    print(
        "最大值与平均值比例：",
        round(skew_ratio, 2),
    )

    print()
    print("数据量最多的前10个值：")

    (
        key_count_df
        .orderBy(
            F.desc("row_count")
        )
        .show(
            10,
            truncate=False,
        )
    )

    if skew_ratio >= 10:
        print(
            "体检结论：存在明显热点值，"
            "执行分组或关联时需要注意数据倾斜"
        )
    elif skew_ratio >= 5:
        print(
            "体检结论：存在一定分布差异，"
            "暂时观察执行时间和任务分布"
        )
    else:
        print(
            "体检结论：当前没有发现明显数据倾斜"
        )

    return {
        "column_name": column_name,
        "null_count": null_count,
        "key_count": key_count,
        "max_rows": max_rows,
        "avg_rows": avg_rows,
        "skew_ratio": skew_ratio,
    }


# =========================================================
# 6. 逐张检查数仓表
# =========================================================

audit_results = {}

for table_name, table_path in TABLE_PATHS.items():
    result = audit_table(
        table_name,
        table_path,
    )

    if result is not None:
        audit_results[
            table_name
        ] = result


# =========================================================
# 7. 检查DWD关键字段的数据分布
# =========================================================

dwd_result = audit_results.get(
    "DWD订单明细表"
)

if dwd_result is not None:
    dwd_df = dwd_result["df"]

    print()
    print("=" * 80)
    print("DWD数据倾斜检查")
    print("=" * 80)

    audit_key_distribution(
        dwd_df,
        "customer_id",
        "客户编号",
    )

    audit_key_distribution(
        dwd_df,
        "product_id",
        "商品编号",
    )

    audit_key_distribution(
        dwd_df,
        "receiver_province_name",
        "收货省份",
    )

    audit_key_distribution(
        dwd_df,
        "order_channel",
        "订单渠道",
    )


# =========================================================
# 8. 检查广播关联候选表
# =========================================================

print()
print("=" * 80)
print("广播关联候选检查")
print("=" * 80)

broadcast_candidates = [
    "ODS客户表",
    "ODS商品表",
]

for table_name in broadcast_candidates:
    result = audit_results.get(
        table_name
    )

    if result is None:
        continue

    print()
    print("候选表：", table_name)
    print("数据行数：", result["row_count"])

    print(
        "文件总大小：",
        round(
            result["total_mb"],
            2,
        ),
        "MB",
    )

    if result["total_mb"] <= 10:
        print(
            "体检建议：当前数据量较小，"
            "可以作为广播关联候选表"
        )
    else:
        print(
            "体检建议：不要仅根据行数广播，"
            "需要继续评估实际内存占用"
        )


# =========================================================
# 9. 输出数仓体检汇总
# =========================================================

print()
print("=" * 80)
print("Spark数仓性能体检汇总")
print("=" * 80)

print(
    "成功检查表数量：",
    len(audit_results),
)

print()
print("后续优化判断规则：")

print(
    "1. 文件数量多且平均文件很小："
    "考虑coalesce或调整写出分区"
)

print(
    "2. 某字段最大值远高于平均值："
    "检查数据倾斜"
)

print(
    "3. 大表关联小型维度表："
    "检查是否适合Broadcast Join"
)

print(
    "4. 同一DataFrame在同一程序中重复使用："
    "考虑cache"
)

print(
    "5. 经常按照月份查询："
    "考虑partitionBy和分区裁剪"
)


# =========================================================
# 10. 停止Spark程序
# =========================================================

spark.stop()

print()
print("Spark数仓性能体检完成")
