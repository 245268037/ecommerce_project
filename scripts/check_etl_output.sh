#!/bin/bash

set -u


# ==============================
# 1. 项目路径
# ==============================

SCRIPT_DIR="$(
    cd "$(dirname "${BASH_SOURCE[0]}")"
    pwd
)"

PROJECT_DIR="$(
    cd "$SCRIPT_DIR/.."
    pwd
)"
MIN_MTIME_EPOCH="${MIN_MTIME_EPOCH:-0}"

if ! [[ "$MIN_MTIME_EPOCH" =~ ^[0-9]+$ ]]; then
    echo "错误：MIN_MTIME_EPOCH必须是非负整数"
    exit 1
fi

# ==============================
# 2. 日志函数
# ==============================

log() {
    local message="$1"

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $message"
}
get_csv_rows() {
    local file_path="$1"

    tail -n +2 "$file_path" |
        wc -l
}
compare_rows() {
    local check_name="$1"
    local source_file="$2"
    local target_file="$3"

    local source_rows
    local target_rows

    source_rows=$(
        get_csv_rows "$source_file"
    )

    target_rows=$(
        get_csv_rows "$target_file"
    )

    if [ "$source_rows" -eq "$target_rows" ]; then
        log "通过：$check_name，源行数=$source_rows，目标行数=$target_rows"
        return 0
    fi

    log "失败：$check_name，源行数=$source_rows，目标行数=$target_rows"

    return 1
}

# ==============================
# 3. 文件和预期行数
# ==============================

REQUIRED_FILES=(
    "warehouse/dwd/dwd_order_detail.csv"
    "warehouse/dws/dws_user_sales.csv"
    "warehouse/dws/dws_product_sales.csv"
    "warehouse/ads/ads_sales_summary.csv"
    "warehouse/ads/ads_product_summary.csv"
    "warehouse/ads/ads_user_rfm_segment.csv"
    "warehouse/bi/bi_fact_order.csv"
    "warehouse/bi/bi_fact_order_detail.csv"
    "warehouse/bi/bi_dim_date.csv"
)


# ==============================
# 4. 逐个检查
# ==============================

failed_count=0

for relative_path in "${REQUIRED_FILES[@]}"
do
    file_path="$PROJECT_DIR/$relative_path"

    if [ ! -f "$file_path" ]; then
        log "失败：文件不存在：$relative_path"
        failed_count=$((failed_count + 1))
        continue
    fi

    if [ ! -r "$file_path" ]; then
        log "失败：文件不可读：$relative_path"
        failed_count=$((failed_count + 1))
        continue
    fi

    if [ ! -s "$file_path" ]; then
        log "失败：文件为空：$relative_path"
        failed_count=$((failed_count + 1))
        continue
    fi
file_mtime=$(
    stat -c %Y "$file_path"
)

if [ "$file_mtime" -lt "$MIN_MTIME_EPOCH" ]; then
    file_mtime_text=$(
        stat -c %y "$file_path"
    )

    log "失败：文件不是本次任务生成：$relative_path，更新时间=$file_mtime_text"
    failed_count=$((failed_count + 1))
    continue
fi

    actual_rows=$(
        tail -n +2 "$file_path" |
        wc -l
    )

    if [ "$actual_rows" -le 0 ]; then
    log "失败：文件没有数据行：$relative_path"
    failed_count=$((failed_count + 1))
    continue
fi

    log "通过：$relative_path，数据行数=$actual_rows"
done


# ==============================
# 5. 返回检查结果
# ==============================

if [ "$failed_count" -gt 0 ]; then
    log "ETL输出检查失败，失败项数量=$failed_count"
    exit 1
fi


# ==============================
# 5. 跨层行数一致性检查
# ==============================

if ! compare_rows \
    "DWD订单明细 → BI订单明细" \
    "$PROJECT_DIR/warehouse/dwd/dwd_order_detail.csv" \
    "$PROJECT_DIR/warehouse/bi/bi_fact_order_detail.csv"
then
    failed_count=$((failed_count + 1))
fi


if ! compare_rows \
    "DWS用户主题 → ADS RFM用户" \
    "$PROJECT_DIR/warehouse/dws/dws_user_sales.csv" \
    "$PROJECT_DIR/warehouse/ads/ads_user_rfm_segment.csv"
then
    failed_count=$((failed_count + 1))
fi
if ! compare_rows \
    "DWS商品主题 → ADS商品指标" \
    "$PROJECT_DIR/warehouse/dws/dws_product_sales.csv" \
    "$PROJECT_DIR/warehouse/ads/ads_product_summary.csv"
then
    failed_count=$((failed_count + 1))
fi


if ! compare_rows \
    "ADS日销售日期 → BI日期维度" \
    "$PROJECT_DIR/warehouse/ads/ads_sales_summary.csv" \
    "$PROJECT_DIR/warehouse/bi/bi_dim_date.csv"
then
    failed_count=$((failed_count + 1))
fi

log "ETL输出文件检查全部通过"
exit 0
