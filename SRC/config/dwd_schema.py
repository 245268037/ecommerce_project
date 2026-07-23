DWD_ORDER_DETAIL_SCHEMA = {

    # 主键和编号
    "order_id": "string",
    "order_no": "string",
    "order_detail_id": "string",
    "customer_id": "string",
    "product_id": "string",
    "shop_id": "string",

    # 业务属性
    "order_status": "string",
    "order_channel": "string",
    "customer_name": "string",
    "member_level": "string",
    "product_name": "string",
    "category_name": "string",

    # 数量和金额
    "quantity": "int64",
    "unit_price": "float64",
    "discount_amount": "float64",
    "actual_amount": "float64",
    "order_amount": "float64",
    "coupon_amount": "float64",
    "freight_amount": "float64",
    "payable_amount": "float64",

    # 时间字段
    "order_time": "string",
    "created_at": "string",

    # 地区字段
    "receiver_province_code": "string",
    "receiver_province_name": "string"

}

DWD_ORDER_COLUMNS = [
     # 订单字段
    "order_id",
    "order_no",
    "customer_id",
    "shop_id",
    "order_time",
    "order_status",
    "order_channel",

    # 订单金额
    "order_amount",
    "coupon_amount",
    "freight_amount",
    "payable_amount",

    # 收货地区
    "receiver_province_code",
    "receiver_province_name",

    # 客户字段
    "customer_name",
    "member_level",

    # 订单明细字段
    "order_detail_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_amount",
    "actual_amount",
    "created_at",

    # 商品维度字段
    "product_name",
    "category_name"
]