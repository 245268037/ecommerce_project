"""
使用Spark构建商品ODS层。

处理过程：
1. 读取RAW商品CSV。
2. 检查价格字段能否转换成正式金额。
3. 检查时间字段能否转换。
4. 转换ODS正式字段类型。
5. 检查商品编号及重要字段。
6. 观察价格和商品状态。
7. 增加ETL技术字段。
8. 保存为Parquet并重新读取验证。
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    lit,
    to_timestamp,
    trim,
)
from pyspark.sql.types import DecimalType

from schemas import PRODUCT_SCHEMA


# =========================================================
# 1. 路径配置
# =========================================================

PROJECT_DIR = Path(__file__).resolve().parents[1]

RAW_PRODUCT_FILE = (
    PROJECT_DIR
    / "data"
    / "raw"
    / "product.csv"
)

ODS_PRODUCT_DIR = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ods"
    / "ods_product"
)


# =========================================================
# 2. 字段配置
# =========================================================

REQUIRED_COLUMNS = [
    "product_id",
    "product_name",
    "category_id",
]

AMOUNT_COLUMNS = [
    "unit_price",
    "cost_price",
]

TIME_COLUMNS = [
    "created_at",
    "updated_at",
]

MONEY_TYPE = DecimalType(
    18,
    2,
)

TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"

EXPECTED_ROWS = 3000


# =========================================================
# 3. 读取RAW商品表
# =========================================================

def read_raw_product(spark):
    """
    使用明确Schema读取商品CSV。
    """
    return (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")
        .option("mode", "FAILFAST")
        .schema(PRODUCT_SCHEMA)
        .csv(str(RAW_PRODUCT_FILE))
    )


# =========================================================
# 4. 检查价格字段
# =========================================================

def check_amount_columns(raw_product):
    """
    检查商品价格能否转换成正式金额。
    """
    failed_count = 0

    for column_name in AMOUNT_COLUMNS:
        invalid_count = (
            raw_product
            .filter(
                col(column_name).isNotNull()
                & (
                    trim(
                        col(column_name)
                    ) != ""
                )
                & (
                    col(column_name)
                    .cast(MONEY_TYPE)
                    .isNull()
                )
            )
            .count()
        )

        print(
            f"价格字段检查：{column_name}，"
            f"转换失败数量={invalid_count}"
        )

        failed_count += invalid_count

    if failed_count > 0:
        raise ValueError(
            "商品表存在无法转换的价格，"
            f"异常数量={failed_count}"
        )


# =========================================================
# 5. 检查时间字段
# =========================================================

def check_time_columns(raw_product):
    """
    检查商品时间字段能否转换。
    """
    failed_count = 0

    for column_name in TIME_COLUMNS:
        invalid_count = (
            raw_product
            .filter(
                col(column_name).isNotNull()
                & (
                    trim(
                        col(column_name)
                    ) != ""
                )
                & (
                    to_timestamp(
                        col(column_name),
                        TIME_FORMAT,
                    ).isNull()
                )
            )
            .count()
        )

        print(
            f"时间字段检查：{column_name}，"
            f"转换失败数量={invalid_count}"
        )

        failed_count += invalid_count

    if failed_count > 0:
        raise ValueError(
            "商品表存在无法转换的时间，"
            f"异常数量={failed_count}"
        )


# =========================================================
# 6. 转换ODS正式类型
# =========================================================

def transform_product(raw_product):
    """
    将RAW商品表转换成ODS商品表。
    """
    ods_product = raw_product

    for column_name in AMOUNT_COLUMNS:
        ods_product = (
            ods_product
            .withColumn(
                column_name,
                col(column_name).cast(
                    MONEY_TYPE
                ),
            )
        )

    for column_name in TIME_COLUMNS:
        ods_product = (
            ods_product
            .withColumn(
                column_name,
                to_timestamp(
                    col(column_name),
                    TIME_FORMAT,
                ),
            )
        )

    ods_product = (
        ods_product
        .withColumn(
            "etl_time",
            current_timestamp(),
        )
        .withColumn(
            "source_system",
            lit("ecommerce"),
        )
        .withColumn(
            "etl_batch",
            date_format(
                current_timestamp(),
                "yyyyMMddHHmmss",
            ),
        )
    )

    return ods_product


# =========================================================
# 7. 检查重要字段
# =========================================================

def check_required_columns(ods_product):
    """
    检查商品编号、商品名称和分类编号。
    """
    failed_count = 0

    for column_name in REQUIRED_COLUMNS:
        null_count = (
            ods_product
            .filter(
                col(column_name).isNull()
                | (
                    trim(
                        col(column_name)
                    ) == ""
                )
            )
            .count()
        )

        print(
            f"重要字段检查：{column_name}，"
            f"空值数量={null_count}"
        )

        failed_count += null_count

    if failed_count > 0:
        raise ValueError(
            "商品表存在重要字段为空，"
            f"异常数量={failed_count}"
        )


# =========================================================
# 8. 检查ODS商品表
# =========================================================

def check_ods_product(ods_product):
    """
    检查商品行数、重要字段及重复商品。
    """
    actual_rows = ods_product.count()

    print(
        f"ODS商品实际行数：{actual_rows}"
    )

    if actual_rows != EXPECTED_ROWS:
        raise ValueError(
            "ODS商品行数不正确："
            f"实际={actual_rows}，"
            f"预期={EXPECTED_ROWS}"
        )

    check_required_columns(
        ods_product
    )

    duplicate_product_count = (
        ods_product
        .groupBy("product_id")
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    print(
        "重复商品编号数量："
        f"{duplicate_product_count}"
    )

    if duplicate_product_count > 0:
        raise ValueError(
            "ODS商品表存在重复商品编号，"
            f"重复编号数量={duplicate_product_count}"
        )

    print(
        "[通过] ODS商品质量检查通过"
    )


# =========================================================
# 9. 观察商品价格
# =========================================================

def show_price_summary(ods_product):
    """
    查看商品价格情况，不修改源数据。
    """
    print(
        "商品价格基本情况："
    )

    (
        ods_product
        .select(
            "unit_price",
            "cost_price",
        )
        .summary(
            "count",
            "min",
            "max",
            "mean",
        )
        .show(
            truncate=False
        )
    )

    negative_price_count = (
        ods_product
        .filter(
            (col("unit_price") < 0)
            | (col("cost_price") < 0)
        )
        .count()
    )

    cost_higher_count = (
        ods_product
        .filter(
            col("cost_price")
            > col("unit_price")
        )
        .count()
    )

    print(
        "价格小于0的商品数："
        f"{negative_price_count}"
    )

    print(
        "成本价高于销售价的商品数："
        f"{cost_higher_count}"
    )


# =========================================================
# 10. 查看商品状态
# =========================================================

def show_product_status(ods_product):
    """
    查看商品状态分布，不修改数据。
    """
    print(
        "商品状态分布："
    )

    (
        ods_product
        .groupBy("product_status")
        .count()
        .orderBy("product_status")
        .show(
            truncate=False
        )
    )


# =========================================================
# 11. 保存并重新读取
# =========================================================

def save_and_verify(
        spark,
        ods_product,
):
    """
    保存Parquet并重新读取核对。
    """
    print(
        "开始保存ODS商品表："
        f"{ODS_PRODUCT_DIR}"
    )

    (
        ods_product.write
        .mode("overwrite")
        .parquet(
            str(ODS_PRODUCT_DIR)
        )
    )

    saved_product = (
        spark.read
        .parquet(
            str(ODS_PRODUCT_DIR)
        )
    )

    saved_rows = saved_product.count()

    print(
        f"Parquet重新读取行数：{saved_rows}"
    )

    if saved_rows != EXPECTED_ROWS:
        raise ValueError(
            "商品Parquet保存前后行数不一致："
            f"实际={saved_rows}，"
            f"预期={EXPECTED_ROWS}"
        )

    print(
        "商品Parquet字段结构："
    )

    saved_product.printSchema()

    print(
        "商品Parquet前2条数据："
    )

    saved_product.show(
        n=2,
        truncate=False,
    )

    print(
        "[通过] ODS商品Parquet保存检查通过"
    )


# =========================================================
# 12. 主程序
# =========================================================

def main():
    spark = (
        SparkSession.builder
        .appName(
            "BuildODSProductParquet"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:
        print(
            "\n========== 读取RAW商品表 =========="
        )

        raw_product = read_raw_product(
            spark
        )

        raw_rows = raw_product.count()

        print(
            f"RAW商品行数：{raw_rows}"
        )

        print(
            "\n========== 检查字段转换 =========="
        )

        check_amount_columns(
            raw_product
        )

        check_time_columns(
            raw_product
        )

        print(
            "\n========== 转换ODS商品表 =========="
        )

        ods_product = transform_product(
            raw_product
        )

        ods_product.printSchema()

        print(
            "\n========== 检查ODS商品表 =========="
        )

        check_ods_product(
            ods_product
        )

        print(
            "\n========== 查看价格情况 =========="
        )

        show_price_summary(
            ods_product
        )

        print(
            "\n========== 查看商品状态 =========="
        )

        show_product_status(
            ods_product
        )

        print(
            "\n========== 保存Parquet =========="
        )

        save_and_verify(
            spark=spark,
            ods_product=ods_product,
        )

        print(
            "\n全部ODS商品表构建完成"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
