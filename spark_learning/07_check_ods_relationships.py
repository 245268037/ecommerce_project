"""
检查Spark版ODS四张表之间的关系。

检查内容：
1. 订单明细是否都能找到订单。
2. 订单是否都能找到客户。
3. 订单明细是否都能找到商品。
4. 每张订单是否至少有一条商品明细。
"""

from pathlib import Path

from pyspark.sql import SparkSession


# =========================================================
# 1. 路径配置
# =========================================================

PROJECT_DIR = Path(__file__).resolve().parents[1]

ODS_DIR = (
    PROJECT_DIR
    / "warehouse_spark"
    / "ods"
)

ODS_ORDER_DIR = (
    ODS_DIR
    / "ods_order"
)

ODS_ORDER_DETAIL_DIR = (
    ODS_DIR
    / "ods_order_detail"
)

ODS_CUSTOMER_DIR = (
    ODS_DIR
    / "ods_customer"
)

ODS_PRODUCT_DIR = (
    ODS_DIR
    / "ods_product"
)


# =========================================================
# 2. 读取ODS表
# =========================================================

def read_ods_tables(spark):
    """
    读取四张ODS Parquet表。
    """
    order = spark.read.parquet(
        str(ODS_ORDER_DIR)
    )

    order_detail = spark.read.parquet(
        str(ODS_ORDER_DETAIL_DIR)
    )

    customer = spark.read.parquet(
        str(ODS_CUSTOMER_DIR)
    )

    product = spark.read.parquet(
        str(ODS_PRODUCT_DIR)
    )

    return {
        "order": order,
        "order_detail": order_detail,
        "customer": customer,
        "product": product,
    }


# =========================================================
# 3. 检查表行数
# =========================================================

def check_table_rows(tables):
    """
    核对四张ODS表的行数。
    """
    expected_rows = {
        "order": 100000,
        "order_detail": 299831,
        "customer": 10000,
        "product": 3000,
    }

    failed_count = 0

    for table_name, expected_count in expected_rows.items():
        actual_count = (
            tables[table_name].count()
        )

        print(
            f"{table_name}："
            f"实际行数={actual_count}，"
            f"预期行数={expected_count}"
        )

        if actual_count != expected_count:
            failed_count += 1

    if failed_count > 0:
        raise ValueError(
            "ODS表行数检查失败，"
            f"失败表数量={failed_count}"
        )

    print(
        "[通过] 四张ODS表行数检查通过"
    )


# =========================================================
# 4. 检查明细找不到订单
# =========================================================

def check_detail_without_order(
        order,
        order_detail,
):
    """
    查找在订单表中找不到订单的明细。
    """
    order_keys = (
        order
        .select("order_id")
    )

    error_data = (
        order_detail
        .join(
            order_keys,
            on="order_id",
            how="left_anti",
        )
    )

    error_count = error_data.count()

    print(
        "找不到订单的明细数量："
        f"{error_count}"
    )

    if error_count > 0:
        print(
            "异常明细示例："
        )

        error_data.show(
            n=10,
            truncate=False,
        )

        raise ValueError(
            "存在找不到订单的商品明细"
        )


# =========================================================
# 5. 检查订单找不到客户
# =========================================================

def check_order_without_customer(
        order,
        customer,
):
    """
    查找在客户表中找不到客户的订单。
    """
    customer_keys = (
        customer
        .select("customer_id")
    )

    error_data = (
        order
        .join(
            customer_keys,
            on="customer_id",
            how="left_anti",
        )
    )

    error_count = error_data.count()

    print(
        "找不到客户的订单数量："
        f"{error_count}"
    )

    if error_count > 0:
        print(
            "异常订单示例："
        )

        error_data.show(
            n=10,
            truncate=False,
        )

        raise ValueError(
            "存在找不到客户的订单"
        )


# =========================================================
# 6. 检查明细找不到商品
# =========================================================

def check_detail_without_product(
        order_detail,
        product,
):
    """
    查找在商品表中找不到商品的订单明细。
    """
    product_keys = (
        product
        .select("product_id")
    )

    error_data = (
        order_detail
        .join(
            product_keys,
            on="product_id",
            how="left_anti",
        )
    )

    error_count = error_data.count()

    print(
        "找不到商品的明细数量："
        f"{error_count}"
    )

    if error_count > 0:
        print(
            "异常商品明细示例："
        )

        error_data.show(
            n=10,
            truncate=False,
        )

        raise ValueError(
            "存在找不到商品的订单明细"
        )


# =========================================================
# 7. 检查没有商品明细的订单
# =========================================================

def check_order_without_detail(
        order,
        order_detail,
):
    """
    查找没有任何商品明细的订单。
    """
    detail_order_keys = (
        order_detail
        .select("order_id")
        .dropDuplicates()
    )

    error_data = (
        order
        .join(
            detail_order_keys,
            on="order_id",
            how="left_anti",
        )
    )

    error_count = error_data.count()

    print(
        "没有商品明细的订单数量："
        f"{error_count}"
    )

    if error_count > 0:
        print(
            "异常订单示例："
        )

        error_data.show(
            n=10,
            truncate=False,
        )

        raise ValueError(
            "存在没有商品明细的订单"
        )


# =========================================================
# 8. 主程序
# =========================================================

def main():
    spark = (
        SparkSession.builder
        .appName(
            "CheckODSRelationships"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:
        print(
            "\n========== 读取ODS表 =========="
        )

        tables = read_ods_tables(
            spark
        )

        print(
            "\n========== 检查表行数 =========="
        )

        check_table_rows(
            tables
        )

        print(
            "\n========== 检查明细与订单 =========="
        )

        check_detail_without_order(
            order=tables["order"],
            order_detail=tables["order_detail"],
        )

        print(
            "\n========== 检查订单与客户 =========="
        )

        check_order_without_customer(
            order=tables["order"],
            customer=tables["customer"],
        )

        print(
            "\n========== 检查明细与商品 =========="
        )

        check_detail_without_product(
            order_detail=tables["order_detail"],
            product=tables["product"],
        )

        print(
            "\n========== 检查订单是否有明细 =========="
        )

        check_order_without_detail(
            order=tables["order"],
            order_detail=tables["order_detail"],
        )

        print(
            "\n全部ODS表关系检查通过"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
