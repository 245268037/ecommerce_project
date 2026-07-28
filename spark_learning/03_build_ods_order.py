"""
使用Spark构建订单明细ODS层。

主要工作：
1. 读取RAW订单明细CSV。
2. 检查数量、金额和时间是否能正确转换。
3. 转换成ODS正式字段类型。
4. 检查关键编号和重复明细。
5. 增加ETL处理信息。
6. 保存为Parquet。
7. 重新读取并核对结果。
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
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
)

from schemas import ORDER_DETAIL_SCHEMA


# =========================================================
# 1. 路径配置
# =========================================================

PROJECT_DIR = Path(__file__).resolve().parents[1]

RAW_ORDER_DETAIL_FILE = (
    PROJECT_DIR
    / "data"
    / "raw"
    / "order_detail.csv"
)

ODS_ORDER_DETAIL_DIR = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ods"
    / "ods_order_detail"
)


# =========================================================
# 2. 字段配置
# =========================================================

KEY_COLUMNS = [
    "order_detail_id",
    "order_id",
    "product_id",
]

AMOUNT_COLUMNS = [
    "unit_price",
    "discount_amount",
    "actual_amount",
]

QUANTITY_COLUMN = "quantity"

TIME_COLUMN = "created_at"

MONEY_TYPE = DecimalType(
    18,
    2,
)

QUANTITY_TYPE = IntegerType()

TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"

EXPECTED_ROWS = 299831


# =========================================================
# 3. 读取RAW订单明细
# =========================================================

def read_raw_order_detail(spark):
    """
    使用明确Schema读取订单明细CSV。
    """
    return (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")
        .option("mode", "FAILFAST")
        .schema(ORDER_DETAIL_SCHEMA)
        .csv(str(RAW_ORDER_DETAIL_FILE))
    )


# =========================================================
# 4. 检查数量字段
# =========================================================

def check_quantity_column(
        raw_order_detail,
):
    """
    检查quantity能否转换成整数。
    """
    invalid_count = (
        raw_order_detail
        .filter(
            col(QUANTITY_COLUMN).isNotNull()
            & (
                trim(
                    col(QUANTITY_COLUMN)
                ) != ""
            )
            & (
                col(QUANTITY_COLUMN)
                .cast(QUANTITY_TYPE)
                .isNull()
            )
        )
        .count()
    )

    print(
        f"数量字段检查：{QUANTITY_COLUMN}，"
        f"转换失败数量={invalid_count}"
    )

    if invalid_count > 0:
        raise ValueError(
            "quantity字段存在无法转换成整数的数据，"
            f"异常数量={invalid_count}"
        )


# =========================================================
# 5. 检查金额字段
# =========================================================

def check_amount_columns(
        raw_order_detail,
):
    """
    检查金额字段能否转换成正式金额。
    """
    failed_count = 0

    for column_name in AMOUNT_COLUMNS:
        invalid_count = (
            raw_order_detail
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
            f"金额字段检查：{column_name}，"
            f"转换失败数量={invalid_count}"
        )

        failed_count += invalid_count

    if failed_count > 0:
        raise ValueError(
            "订单明细金额字段存在无法转换的数据，"
            f"异常数量={failed_count}"
        )


# =========================================================
# 6. 检查时间字段
# =========================================================

def check_time_column(
        raw_order_detail,
):
    """
    检查created_at能否转换成正式时间。
    """
    invalid_count = (
        raw_order_detail
        .filter(
            col(TIME_COLUMN).isNotNull()
            & (
                trim(
                    col(TIME_COLUMN)
                ) != ""
            )
            & (
                to_timestamp(
                    col(TIME_COLUMN),
                    TIME_FORMAT,
                ).isNull()
            )
        )
        .count()
    )

    print(
        f"时间字段检查：{TIME_COLUMN}，"
        f"转换失败数量={invalid_count}"
    )

    if invalid_count > 0:
        raise ValueError(
            "created_at字段存在无法转换的时间，"
            f"异常数量={invalid_count}"
        )


# =========================================================
# 7. 转换ODS正式类型
# =========================================================

def transform_order_detail(
        raw_order_detail,
):
    """
    将RAW订单明细转换为ODS订单明细。
    """
    ods_order_detail = raw_order_detail

    ods_order_detail = (
        ods_order_detail
        .withColumn(
            QUANTITY_COLUMN,
            col(QUANTITY_COLUMN).cast(
                QUANTITY_TYPE
            ),
        )
    )

    for column_name in AMOUNT_COLUMNS:
        ods_order_detail = (
            ods_order_detail
            .withColumn(
                column_name,
                col(column_name).cast(
                    MONEY_TYPE
                ),
            )
        )

    ods_order_detail = (
        ods_order_detail
        .withColumn(
            TIME_COLUMN,
            to_timestamp(
                col(TIME_COLUMN),
                TIME_FORMAT,
            ),
        )
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

    return ods_order_detail


# =========================================================
# 8. 检查关键字段
# =========================================================

def check_key_columns(
        ods_order_detail,
):
    """
    检查明细编号、订单编号和商品编号是否为空。
    """
    failed_count = 0

    for column_name in KEY_COLUMNS:
        null_count = (
            ods_order_detail
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
            f"关键字段检查：{column_name}，"
            f"空值数量={null_count}"
        )

        failed_count += null_count

    if failed_count > 0:
        raise ValueError(
            "订单明细存在关键字段为空，"
            f"异常数量={failed_count}"
        )


# =========================================================
# 9. 检查ODS结果
# =========================================================

def check_ods_order_detail(
        ods_order_detail,
):
    """
    检查ODS订单明细行数、关键字段和重复明细。
    """
    actual_rows = (
        ods_order_detail.count()
    )

    print(
        f"ODS订单明细实际行数：{actual_rows}"
    )

    if actual_rows != EXPECTED_ROWS:
        raise ValueError(
            "ODS订单明细行数不正确："
            f"实际={actual_rows}，"
            f"预期={EXPECTED_ROWS}"
        )

    check_key_columns(
        ods_order_detail
    )

    duplicate_detail_count = (
        ods_order_detail
        .groupBy("order_detail_id")
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    print(
        "重复订单明细编号数量："
        f"{duplicate_detail_count}"
    )

    if duplicate_detail_count > 0:
        raise ValueError(
            "ODS存在重复订单明细编号，"
            f"重复编号数量={duplicate_detail_count}"
        )

    print(
        "[通过] ODS订单明细质量检查通过"
    )


# =========================================================
# 10. 保存并重新读取
# =========================================================

def save_and_verify(
        spark,
        ods_order_detail,
):
    """
    保存Parquet，并重新读取核对。
    """
    print(
        "开始保存ODS订单明细："
        f"{ODS_ORDER_DETAIL_DIR}"
    )

    (
        ods_order_detail.write
        .mode("overwrite")
        .parquet(
            str(ODS_ORDER_DETAIL_DIR)
        )
    )

    saved_order_detail = (
        spark.read
        .parquet(
            str(ODS_ORDER_DETAIL_DIR)
        )
    )

    saved_rows = (
        saved_order_detail.count()
    )

    print(
        f"Parquet重新读取行数：{saved_rows}"
    )

    if saved_rows != EXPECTED_ROWS:
        raise ValueError(
            "订单明细Parquet保存前后行数不一致："
            f"实际={saved_rows}，"
            f"预期={EXPECTED_ROWS}"
        )

    print(
        "订单明细Parquet字段结构："
    )

    saved_order_detail.printSchema()

    print(
        "订单明细Parquet前2条数据："
    )

    saved_order_detail.show(
        n=2,
        truncate=False,
    )

    print(
        "[通过] ODS订单明细Parquet保存检查通过"
    )


# =========================================================
# 11. 主程序
# =========================================================

def main():
    spark = (
        SparkSession.builder
        .appName(
            "BuildODSOrderDetailParquet"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:
        print(
            "\n========== 读取RAW订单明细 =========="
        )

        raw_order_detail = (
            read_raw_order_detail(
                spark
            )
        )

        raw_rows = (
            raw_order_detail.count()
        )

        print(
            f"RAW订单明细行数：{raw_rows}"
        )

        print(
            "\n========== 检查字段转换 =========="
        )

        check_quantity_column(
            raw_order_detail
        )

        check_amount_columns(
            raw_order_detail
        )

        check_time_column(
            raw_order_detail
        )

        print(
            "\n========== 转换ODS正式类型 =========="
        )

        ods_order_detail = (
            transform_order_detail(
                raw_order_detail
            )
        )

        ods_order_detail.printSchema()

        print(
            "\n========== 检查ODS订单明细 =========="
        )

        check_ods_order_detail(
            ods_order_detail
        )

        print(
            "\n========== 保存Parquet =========="
        )

        save_and_verify(
            spark=spark,
            ods_order_detail=ods_order_detail,
        )

        print(
            "\n全部ODS订单明细构建完成"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
