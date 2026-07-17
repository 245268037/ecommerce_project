# config/ads_schema.py
#销售日报
ADS_SALES_SUMMARY_SCHEMA = {

    "order_date": "string",

    "order_count": "Int64",

    "user_count": "Int64",

    "sales_amount": "float64",

    "avg_order_amount": "float64"
}

#用户运营
ADS_USER_SUMMARY_SCHEMA = {

    "customer_id": "string",

    "customer_name": "string",

    "order_count": "Int64",

    "total_amount": "float64",

    "avg_amount": "float64",

    "user_level": "string"
}

#商品运营
ADS_PRODUCT_SUMMARY_SCHEMA = {

    "product_id": "string",

    "product_name": "string",

    "category_name": "string",

    "sales_count": "Int64",

    "sales_amount": "float64",

    "rank": "Int64"
}