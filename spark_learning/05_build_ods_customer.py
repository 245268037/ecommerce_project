"""
使用Spark构建客户ODS层。

处理过程：
1. 读取RAW客户CSV。
2. 检查年龄是否能够转换成整数。
3. 检查时间是否能够转换。
4. 转换ODS正式字段类型。
5. 检查客户编号是否为空、是否重复。
6. 增加ETL技术字段。
7. 保存为Parquet。
8. 重新读取并核对。
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
from pyspark.sql.types import IntegerType

from schemas import CUSTOMER_SCHEMA


# =========================================================
# 1. 路径配置
# =========================================================

PROJECT_DIR = Path(__file__).resolve().parents[1]

RAW_CUSTOMER_FILE = (
    PROJECT_DIR
    / "data"
    / "raw"
    / "customer.csv"
)

ODS_CUSTOMER_DIR = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ods"
    / "ods_customer"
)


# =========================================================
# 2. 字段配置
# =========================================================

AGE_COLUMN = "age"

AGE_TYPE = IntegerType()

TIME_COLUMNS = [
    "register_time",
    "updated_at",
]

TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"

EXPECTED_ROWS = 10000


# =========================================================
# 3. 读取RAW客户表
# =========================================================

def read_raw_customer(spark):
    """
    使用明确Schema读取客户CSV。
    """
    return (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")
        .option("mode", "FAILFAST")
        .schema(CUSTOMER_SCHEMA)
        .csv(str(RAW_CUSTOMER_FILE))
    )


# =========================================================
# 4. 检查年龄字段
# =========================================================

def check_age_column(raw_customer):
    """
    检查age是否能够转换成整数。
    """
    invalid_count = (
        raw_customer
        .filter(
            col(AGE_COLUMN).isNotNull()
            & (
                trim(
                    col(AGE_COLUMN)
                ) != ""
            )
            & (
                col(AGE_COLUMN)
                .cast(AGE_TYPE)
                .isNull()
            )
        )
        .count()
    )

    print(
        f"年龄字段检查：{AGE_COLUMN}，"
        f"转换失败数量={invalid_count}"
    )

    if invalid_count > 0:
        raise ValueError(
            "age字段存在无法转换成整数的数据，"
            f"异常数量={invalid_count}"
        )


# =========================================================
# 5. 检查时间字段
# =========================================================

def check_time_columns(raw_customer):
    """
    检查客户时间字段能否转换。
    """
    failed_count = 0

    for column_name in TIME_COLUMNS:
        invalid_count = (
            raw_customer
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
            "客户表存在无法转换的时间，"
            f"异常数量={failed_count}"
        )


# =========================================================
# 6. 转换ODS正式类型
# =========================================================

def transform_customer(raw_customer):
    """
    将RAW客户表转换为ODS客户表。
    """
    ods_customer = (
        raw_customer
        .withColumn(
            AGE_COLUMN,
            col(AGE_COLUMN).cast(
                AGE_TYPE
            ),
        )
    )

    for column_name in TIME_COLUMNS:
        ods_customer = (
            ods_customer
            .withColumn(
                column_name,
                to_timestamp(
                    col(column_name),
                    TIME_FORMAT,
                ),
            )
        )

    ods_customer = (
        ods_customer
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

    return ods_customer


# =========================================================
# 7. 检查客户表
# =========================================================

def check_ods_customer(ods_customer):
    """
    检查客户行数、空客户编号和重复客户。
    """
    actual_rows = (
        ods_customer.count()
    )

    null_customer_id_count = (
        ods_customer
        .filter(
            col("customer_id").isNull()
            | (
                trim(
                    col("customer_id")
                ) == ""
            )
        )
        .count()
    )

    duplicate_customer_count = (
        ods_customer
        .groupBy("customer_id")
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    print(
        f"ODS客户实际行数：{actual_rows}"
    )
    print(
        "客户编号为空数量："
        f"{null_customer_id_count}"
    )
    print(
        "重复客户编号数量："
        f"{duplicate_customer_count}"
    )

    if actual_rows != EXPECTED_ROWS:
        raise ValueError(
            "ODS客户行数不正确："
            f"实际={actual_rows}，"
            f"预期={EXPECTED_ROWS}"
        )

    if null_customer_id_count > 0:
        raise ValueError(
            "ODS客户表存在空客户编号"
        )

    if duplicate_customer_count > 0:
        raise ValueError(
            "ODS客户表存在重复客户编号"
        )

    print(
        "[通过] ODS客户质量检查通过"
    )


# =========================================================
# 8. 输出年龄基本情况
# =========================================================

def show_age_summary(ods_customer):
    """
    查看客户年龄的基本情况，不修改数据。
    """
    print(
        "客户年龄基本情况："
    )

    (
        ods_customer
        .select("age")
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

    unusual_age_count = (
        ods_customer
        .filter(
            col("age").isNotNull()
            & (
                (col("age") < 0)
                | (col("age") > 120)
            )
        )
        .count()
    )

    print(
        "年龄小于0或大于120的客户数："
        f"{unusual_age_count}"
    )


# =========================================================
# 9. 保存并重新读取
# =========================================================

def save_and_verify(
        spark,
        ods_customer,
):
    """
    保存Parquet并重新读取核对。
    """
    print(
        "开始保存ODS客户表："
        f"{ODS_CUSTOMER_DIR}"
    )

    (
        ods_customer.write
        .mode("overwrite")
        .parquet(
            str(ODS_CUSTOMER_DIR)
        )
    )

    saved_customer = (
        spark.read
        .parquet(
            str(ODS_CUSTOMER_DIR)
        )
    )

    saved_rows = (
        saved_customer.count()
    )

    print(
        f"Parquet重新读取行数：{saved_rows}"
    )

    if saved_rows != EXPECTED_ROWS:
        raise ValueError(
            "客户Parquet保存前后行数不一致："
            f"实际={saved_rows}，"
            f"预期={EXPECTED_ROWS}"
        )

    print(
        "客户Parquet字段结构："
    )

    saved_customer.printSchema()

    print(
        "客户Parquet前2条数据："
    )

    saved_customer.show(
        n=2,
        truncate=False,
    )

    print(
        "[通过] ODS客户Parquet保存检查通过"
    )


# =========================================================
# 10. 主程序
# =========================================================

def main():
    spark = (
        SparkSession.builder
        .appName(
            "BuildODSCustomerParquet"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:
        print(
            "\n========== 读取RAW客户表 =========="
        )

        raw_customer = (
            read_raw_customer(
                spark
            )
        )

        raw_rows = raw_customer.count()

        print(
            f"RAW客户行数：{raw_rows}"
        )

        print(
            "\n========== 检查字段转换 =========="
        )

        check_age_column(
            raw_customer
        )

        check_time_columns(
            raw_customer
        )

        print(
            "\n========== 转换ODS客户表 =========="
        )

        ods_customer = (
            transform_customer(
                raw_customer
            )
        )

        ods_customer.printSchema()

        print(
            "\n========== 检查ODS客户表 =========="
        )

        check_ods_customer(
            ods_customer
        )

        print(
            "\n========== 查看年龄情况 =========="
        )

        show_age_summary(
            ods_customer
        )

        print(
            "\n========== 保存Parquet =========="
        )

        save_and_verify(
            spark=spark,
            ods_customer=ods_customer,
        )

        print(
            "\n全部ODS客户表构建完成"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
