import os


# ==================================================
# 1. 项目根目录
# ==================================================

# 当前文件:
# ECommerce_Project/SRC/config/path.py

CURRENT_DIR = os.path.dirname(
    __file__
)


BASE_DIR = os.path.dirname(
    os.path.dirname(
        CURRENT_DIR
    )
)


# ==================================================
# 2. 基础目录
# ==================================================

DATA_DIR = os.path.join(
    BASE_DIR,
    "data"
)


WAREHOUSE_DIR = os.path.join(
    BASE_DIR,
    "warehouse"
)


LOG_DIR = os.path.join(
    BASE_DIR,
    "logs"
)



# ==================================================
# 3. 数据目录
# ==================================================

# 原始层
RAW_DIR = os.path.join(
    DATA_DIR,
    "raw"
)


# 清洗层
CLEAN_DIR = os.path.join(
    DATA_DIR,
    "clean"
)


# 报告
REPORT_DIR = os.path.join(
    DATA_DIR,
    "report"
)


# 元数据
METADATA_DIR = os.path.join(
    DATA_DIR,
    "metadata"
)



# ==================================================
# 4. RAW 文件
# ==================================================

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



# ==================================================
# 5. CLEAN 文件
# ==================================================

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



# ==================================================
# 6. 数仓 ODS
# ==================================================

ODS_DIR = os.path.join(
    WAREHOUSE_DIR,
    "ods"
)


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



# ==================================================
# 7. 数仓 DWD
# ==================================================

DWD_DIR = os.path.join(
    WAREHOUSE_DIR,
    "dwd"
)


DWD_ORDER_DETAIL = os.path.join(
    DWD_DIR,
    "dwd_order_detail.csv"
)



# ==================================================
# 8. 数仓 DWS
# ==================================================

DWS_DIR = os.path.join(
    WAREHOUSE_DIR,
    "dws"
)


DWS_USER_SALES = os.path.join(
    DWS_DIR,
    "dws_user_sales.csv"
)


DWS_PRODUCT_SALES = os.path.join(
    DWS_DIR,
    "dws_product_sales.csv"
)


DWS_AREA_SALES = os.path.join(
    DWS_DIR,
    "dws_area_sales.csv"
)



# ==================================================
# 9. 数仓 ADS
# ==================================================

ADS_DIR = os.path.join(
    WAREHOUSE_DIR,
    "ads"
)


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



# ==================================================
# 10. 数据质量报告
# ==================================================

QUALITY_REPORT = os.path.join(
    REPORT_DIR,
    "quality_report.xlsx"
)


QUALITY_SCORE = os.path.join(
    REPORT_DIR,
    "quality_score.xlsx"
)


ERROR_REPORT = os.path.join(
    REPORT_DIR,
    "error_detail.xlsx"
)



# ==================================================
# 11. Metadata
# ==================================================

# 数据质量历史

QUALITY_HISTORY = os.path.join(
    METADATA_DIR,
    "quality_history.csv"
)



# ETL运行记录

ETL_JOB_LOG = os.path.join(
    METADATA_DIR,
    "etl_job_log.csv"
)



# ==================================================
# 12. 自动创建目录
# ==================================================

DIR_LIST = [

    DATA_DIR,

    RAW_DIR,

    CLEAN_DIR,

    REPORT_DIR,

    METADATA_DIR,

    WAREHOUSE_DIR,

    ODS_DIR,

    DWD_DIR,

    DWS_DIR,

    ADS_DIR,

    LOG_DIR

]


for directory in DIR_LIST:

    os.makedirs(
        directory,
        exist_ok=True
    )



# ==================================================
# 测试
# ==================================================

if __name__ == "__main__":


    print("项目根目录:")
    print(BASE_DIR)


    print("\n原始订单:")
    print(RAW_ORDER)


    print("\nODS订单:")
    print(ODS_ORDER)


    print("\nDWD:")
    print(DWD_ORDER_DETAIL)


    print("\n质量历史:")
    print(QUALITY_HISTORY)
