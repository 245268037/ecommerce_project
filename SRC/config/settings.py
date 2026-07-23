"""
项目全局配置
"""

# ==========================
# 日志配置
# ==========================

LOG_LEVEL = "INFO"

LOG_FORMAT = (
    "%(asctime)s - %(levelname)s - %(message)s"
)

LOG_FILE = "etl.log"


# ==========================
# CSV配置
# ==========================

CSV_ENCODING = "utf-8-sig"
CSV_INDEX = False
CSV_LOW_MEMORY = False

# ==========================
# Excel配置
# ==========================

EXCEL_ENGINE = "openpyxl"

# ==========================
# 时间配置
# ==========================

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"

# ==========================
# ETL配置
# ==========================

SOURCE_SYSTEM = "ECommerce"
FILE_ENCODING = "utf-8-sig"
ETL_BATCH_SIZE = 10000
# 环境
ENV = "dev"
# 是否开启日志
ENABLE_LOG = True

# ==========================
# 默认dtype
# ==========================

DEFAULT_DTYPE = {
    "receiver_province_code": str,
    "customer_id": str,
    "product_id": str
}
#增加ETL时间
TECH_COLUMNS=[
    "etl_time",
    "source_system",
    "etl_batch"
]

#客户登记区分
VIP_AMOUNT=10000
IMPORTANT_AMOUNT=5000

#
VALID_SALES_STATUSES = (
    "已支付",
    "已发货",
    "已完成"
)