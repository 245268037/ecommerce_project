import os


# 当前文件路径
CURRENT_DIR = os.path.dirname(__file__)


# 项目根目录
BASE_DIR = os.path.dirname(
    os.path.dirname(
        CURRENT_DIR
    )
)




# =========================
# 数据目录
# =========================

RAW_DIR = os.path.join(
    BASE_DIR,
    "data",
    "raw"
)


CLEAN_DIR = os.path.join(
    BASE_DIR,
    "data",
    "clean"
)


REPORT_DIR = os.path.join(
    BASE_DIR,
    "data",
    "report"
)



# =========================
# 数仓目录
# =========================

ODS_DIR = os.path.join(
    BASE_DIR,
    "warehouse",
    "ods"
)


DWD_DIR = os.path.join(
    BASE_DIR,
    "warehouse",
    "dwd"
)


DWS_DIR = os.path.join(
    BASE_DIR,
    "warehouse",
    "dws"
)

DWS_PRODUCT_SALES = os.path.join(
    DWS_DIR,
    "dws_product_sales.csv"
)


ADS_DIR = os.path.join(
    BASE_DIR,
    "warehouse",
    "ads"
)



# =========================
# RAW文件
# =========================


RAW_ORDER = os.path.join(
    RAW_DIR,
    "order.csv"
)


RAW_ORDER_DETAIL = os.path.join(
    RAW_DIR,
    "order_detail.csv"
)


RAW_CUSTOMER = os.path.join(
    RAW_DIR,
    "customer.csv"
)


RAW_PRODUCT = os.path.join(
    RAW_DIR,
    "product.csv"
)



# =========================
# CLEAN文件
# =========================


CLEAN_ORDER = os.path.join(
    CLEAN_DIR,
    "cleaner_order.csv"
)


CLEAN_ORDER_DETAIL = os.path.join(
    CLEAN_DIR,
    "cleaner_order_detail.csv"
)


CLEAN_CUSTOMER = os.path.join(
    CLEAN_DIR,
    "cleaner_customer.csv"
)


CLEAN_PRODUCT = os.path.join(
    CLEAN_DIR,
    "cleaner_product.csv"
)



# =========================
# ODS文件
# =========================


ODS_ORDER = os.path.join(
    ODS_DIR,
    "ods_order.csv"
)


ODS_ORDER_DETAIL = os.path.join(
    ODS_DIR,
    "ods_order_detail.csv"
)


ODS_CUSTOMER = os.path.join(
    ODS_DIR,
    "ods_customer.csv"
)


ODS_PRODUCT = os.path.join(
    ODS_DIR,
    "ods_product.csv"
)



# =========================
# DWD文件
# =========================


DWD_ORDER_DETAIL = os.path.join(
    DWD_DIR,
    "dwd_order_detail.csv"
)



# =========================
# DWS文件
# =========================


DWS_USER_SALES = os.path.join(
    DWS_DIR,
    "dws_user_sales.csv"
)



# =========================
# ADS文件
# =========================


ADS_SALES_SUMMARY = os.path.join(
    ADS_DIR,
    "ads_sales_summary.csv"
)


ADS_USER_SUMMARY = os.path.join(
    ADS_DIR,
    "ads_user_summary.csv"
)


ADS_PRODUCT_SUMMARY = os.path.join(
    ADS_DIR,
    "ads_product_summary.csv"
)