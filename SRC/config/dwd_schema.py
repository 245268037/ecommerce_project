DWD_ORDER_DETAIL_SCHEMA = {


"order_id":"string",

"order_no":"string",

"customer_id":"string",

"customer_name":"string",

"product_id":"string",

"product_name":"string",

"category_name":"string",

"quantity":"int64",

"actual_amount":"float64",

"order_time":"string"

}

DWD_ORDER_COLUMNS = [
    "order_id",
    "order_no",
    "order_time",
    "order_status",
    "customer_id",
    "customer_name",
    "member_level",
    "product_id",
    "product_name",
    "category_name",
    "quantity",
    "actual_amount",
    'payable_amount',
    "receiver_province_code",
    "receiver_province_name"

]