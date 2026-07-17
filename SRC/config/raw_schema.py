"""
RAW层字段类型配置。

设计原则：
1. RAW层以保留源数据为主。
2. 编码、ID、时间、金额等字段先按string读取。
3. 数值、日期类型在DataCleaner中统一转换。
4. Schema字段名必须与CSV表头完全一致。
"""


# =========================================================
# 订单主表 order.csv
# =========================================================

ORDER_RAW_SCHEMA = {
    "order_id": "string",
    "order_no": "string",
    "customer_id": "string",
    "shop_id": "string",

    "order_time": "string",
    "order_status": "string",

    "order_amount": "string",
    "coupon_amount": "string",
    "freight_amount": "string",
    "payable_amount": "string",

    "order_channel": "string",

    "receiver_province_code": "string",
    "receiver_province_name": "string",

    "updated_at": "string"
}


# =========================================================
# 订单明细表 order_detail.csv
# =========================================================

ORDER_DETAIL_RAW_SCHEMA = {
    "order_detail_id": "string",
    "order_id": "string",
    "product_id": "string",

    "quantity": "string",
    "unit_price": "string",
    "discount_amount": "string",
    "actual_amount": "string",

    "created_at": "string"
}


# =========================================================
# 客户表 customer.csv
# =========================================================

CUSTOMER_RAW_SCHEMA = {
    "customer_id": "string",
    "customer_name": "string",

    "phone": "string",
    "email": "string",

    "gender": "string",
    "age": "string",

    "province_code": "string",
    "province_name": "string",

    "member_level": "string",
    "id_card": "string",

    "register_time": "string",
    "updated_at": "string"
}


# =========================================================
# 商品表 product.csv
# =========================================================

PRODUCT_RAW_SCHEMA = {
    "product_id": "string",
    "product_name": "string",
    "category_name": "string",

    # 下面这些字段必须根据你的实际product.csv确认
    "category_code": "string",
    "brand_name": "string",
    "unit_price": "string",
    "product_status": "string",

    "created_at": "string",
    "updated_at": "string"
}