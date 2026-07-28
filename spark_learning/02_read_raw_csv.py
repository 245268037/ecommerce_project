"""
使用Spark读取电商项目RAW层CSV。

本程序负责：
1. 检查CSV表头和配置字段是否一致。
2. 使用明确的Schema读取数据。
3. 检查各表行数。
4. 显示字段结构、分区数量和样例数据。
"""

import csv
from pathlib import Path

from pyspark.sql import SparkSession

from schemas import (
    ORDER_COLUMNS,
    ORDER_DETAIL_COLUMNS,
    CUSTOMER_COLUMNS,
    PRODUCT_COLUMNS,
    ORDER_SCHEMA,
    ORDER_DETAIL_SCHEMA,
    CUSTOMER_SCHEMA,
    PRODUCT_SCHEMA,
)


# =========================================================
# 1. 项目路径
# =========================================================

PROJECT_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = PROJECT_DIR / "data" / "raw"


# =========================================================
# 2. 表配置
# =========================================================

TABLE_CONFIGS = {
    "order": {
        "file_path": RAW_DIR / "order.csv",
        "columns": ORDER_COLUMNS,
        "schema": ORDER_SCHEMA,
        "expected_rows": 100000,
    },
    "order_detail": {
        "file_path": RAW_DIR / "order_detail.csv",
        "columns": ORDER_DETAIL_COLUMNS,
        "schema": ORDER_DETAIL_SCHEMA,
        "expected_rows": 299831,
    },
    "customer": {
        "file_path": RAW_DIR / "customer.csv",
        "columns": CUSTOMER_COLUMNS,
        "schema": CUSTOMER_SCHEMA,
        "expected_rows": 10000,
    },
    "product": {
        "file_path": RAW_DIR / "product.csv",
        "columns": PRODUCT_COLUMNS,
        "schema": PRODUCT_SCHEMA,
        "expected_rows": 3000,
    },
}


# =========================================================
# 3. 检查CSV表头
# =========================================================

def read_csv_header(file_path):
    """
    使用Python读取CSV真实表头。
    """
    with file_path.open(
        mode="r",
        encoding="utf-8-sig",
        newline="",
    ) as file:
        reader = csv.reader(file)
        return next(reader)


def check_header(
        table_name,
        file_path,
        expected_columns,
):
    """
    检查CSV真实表头和Schema字段是否完全一致。
    """
    actual_columns = read_csv_header(
        file_path
    )

    if actual_columns != expected_columns:
        print(
            f"[失败] {table_name}表头与Schema不一致"
        )
        print(
            "CSV字段：",
            actual_columns,
        )
        print(
            "配置字段：",
            expected_columns,
        )

        raise ValueError(
            f"{table_name}字段检查失败"
        )

    print(
        f"[通过] {table_name}字段检查通过，"
        f"字段数={len(actual_columns)}"
    )


# =========================================================
# 4. Spark读取函数
# =========================================================

def read_raw_csv(
        spark,
        file_path,
        schema,
):
    """
    使用明确Schema读取RAW层CSV。
    """
    return (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(str(file_path))
    )


# =========================================================
# 5. 主程序
# =========================================================

def main():
    spark = (
        SparkSession.builder
        .appName("ReadEcommerceRawCSV")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print(
        f"\n项目目录：{PROJECT_DIR}"
    )
    print(
        f"RAW目录：{RAW_DIR}"
    )

    dataframes = {}

    try:
        for table_name, config in TABLE_CONFIGS.items():
            file_path = config["file_path"]

            print(
                "\n"
                "========================================"
            )
            print(
                f"开始检查表：{table_name}"
            )
            print(
                f"文件路径：{file_path}"
            )

            if not file_path.is_file():
                raise FileNotFoundError(
                    f"文件不存在：{file_path}"
                )

            check_header(
                table_name=table_name,
                file_path=file_path,
                expected_columns=config["columns"],
            )

            dataframe = read_raw_csv(
                spark=spark,
                file_path=file_path,
                schema=config["schema"],
            )

            actual_rows = dataframe.count()
            expected_rows = config["expected_rows"]

            print(
                f"实际行数：{actual_rows}"
            )
            print(
                f"预期行数：{expected_rows}"
            )

            if actual_rows != expected_rows:
                raise ValueError(
                    f"{table_name}数据行数不一致："
                    f"实际={actual_rows}，"
                    f"预期={expected_rows}"
                )

            print(
                f"[通过] {table_name}行数检查通过"
            )

            print(
                f"数据分区数："
                f"{dataframe.rdd.getNumPartitions()}"
            )

            print(
                "字段结构："
            )
            dataframe.printSchema()

            print(
                "前2条数据："
            )
            dataframe.show(
                n=2,
                truncate=False,
            )

            dataframes[table_name] = dataframe

        print(
            "\n========================================"
        )
        print(
            "全部RAW层CSV读取检查通过"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
