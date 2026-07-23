#!/bin/bash

set -u


# ==============================
# 1. 路径和参数
# ==============================

SCRIPT_DIR="$(
    cd "$(dirname "${BASH_SOURCE[0]}")"
    pwd
)"

PROJECT_DIR="$(
    cd "$SCRIPT_DIR/.."
    pwd
)"

LOG_DIR="$PROJECT_DIR/logs"

RETENTION_DAYS="${1:-30}"
RUN_MODE="${2:---preview}"


# ==============================
# 2. 日志函数
# ==============================

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}


# ==============================
# 3. 参数检查
# ==============================

if ! [[ "$RETENTION_DAYS" =~ ^[0-9]+$ ]]; then
    log "失败：保留天数必须是非负整数"
    exit 1
fi

if [ ! -d "$LOG_DIR" ]; then
    log "失败：日志目录不存在：$LOG_DIR"
    exit 1
fi


# ==============================
# 4. 查找历史运行日志
# ==============================

log "日志目录：$LOG_DIR"
log "保留天数：$RETENTION_DAYS"

if [ "$RUN_MODE" = "--delete" ]; then
    log "开始删除超过保留期限的ETL运行日志"

    find "$LOG_DIR" \
        -maxdepth 1 \
        -type f \
        -name "etl_run_*.log" \
        -mtime +"$RETENTION_DAYS" \
        -print \
        -delete

    log "日志清理完成"
    exit 0
fi


# ==============================
# 5. 默认只预览
# ==============================

log "当前为预览模式，不会删除文件"
log "以下文件符合清理条件："

find "$LOG_DIR" \
    -maxdepth 1 \
    -type f \
    -name "etl_run_*.log" \
    -mtime +"$RETENTION_DAYS" \
    -print

log "预览完成"

exit 0
