# config/ods_schema.py

ODS_ORDER_SCHEMA = {
    "order_id": "string",
    "order_no": "string",
    "customer_id": "string",
    "shop_id": "string",

    "order_time": "string",
    "order_status": "string",

    "order_amount": "float64",
    "coupon_amount": "float64",
    "freight_amount": "float64",
    "payable_amount": "float64",

    "order_channel": "string",

    "receiver_province_code": "string",
    "receiver_province_name": "string",

    "updated_at": "string",

    # ETL字段
    "etl_time": "string",
    "source_system": "string",
    "etl_batch": "string"
}
#明细
ODS_ORDER_DETAIL_SCHEMA = {


    "order_detail_id":"string",

    "order_id":"string",

    "product_id":"string",


    "quantity":"int64",

    "unit_price":"float64",

    "discount_amount":"float64",

    "actual_amount":"float64",


    "created_at":"string",


    "etl_time":"string",

    "source_system":"string",

    "etl_batch":"string"

}

#客户
ODS_CUSTOMER_SCHEMA = {
    "customer_id": "string",
    "customer_name": "string",
    "phone": "string",
    "email": "string",
    "gender": "string",
    "age": "Int64",
    "province_code": "string",
    "province_name": "string",
    "member_level": "string",
    "register_time": "string",

    "etl_time": "string",
    "source_system": "string",
    "etl_batch": "string"
}

#商品
ODS_PRODUCT_SCHEMA = {
    "product_id": "string",
    "product_name": "string",
    "category_name": "string",
    "brand_name": "string",
    "price": "float64",

    "etl_time": "string",
    "source_system": "string",
    "etl_batch": "string"
}