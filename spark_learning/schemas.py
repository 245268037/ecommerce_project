"""
Spark读取RAW层CSV时使用的字段结构。

设计原则：
1. 字段名称和顺序必须与CSV表头完全一致。
2. RAW层暂时全部按照字符串读取。
3. 金额、数量、日期在ODS加工时再转换。
"""

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
)


def build_string_schema(columns):
    """
    根据字段名称列表，创建全部为字符串的Spark字段结构。
    """
    return StructType([
        StructField(
            column,
            StringType(),
            True,
        )
        for column in columns
    ])


# =========================================================
# 订单主表
# =========================================================

ORDER_COLUMNS = [
    "order_id",
    "order_no",
    "customer_id",
    "shop_id",
    "order_time",
    "order_status",
    "order_amount",
    "coupon_amount",
    "freight_amount",
    "payable_amount",
    "order_channel",
    "receiver_province_code",
    "receiver_province_name",
    "updated_at",
]

ORDER_SCHEMA = build_string_schema(
    ORDER_COLUMNS
)


# =========================================================
# 订单明细表
# =========================================================

ORDER_DETAIL_COLUMNS = [
    "order_detail_id",
    "order_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_amount",
    "actual_amount",
    "created_at",
]

ORDER_DETAIL_SCHEMA = build_string_schema(
    ORDER_DETAIL_COLUMNS
)


# =========================================================
# 客户表
# =========================================================

CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_name",
    "gender",
    "age",
    "phone",
    "email",
    "register_time",
    "member_level",
    "province_code",
    "province_name",
    "customer_status",
    "updated_at",
]

CUSTOMER_SCHEMA = build_string_schema(
    CUSTOMER_COLUMNS
)


# =========================================================
# 商品表
# =========================================================

PRODUCT_COLUMNS = [
    "product_id",
    "product_name",
    "category_id",
    "category_name",
    "brand_name",
    "unit_price",
    "cost_price",
    "product_status",
    "created_at",
    "updated_at",
]

PRODUCT_SCHEMA = build_string_schema(
    PRODUCT_COLUMNS
)
