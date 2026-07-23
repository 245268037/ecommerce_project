#!/bin/bash

set -u


# ==============================
# 1. 路径和阈值
# ==============================

SCRIPT_DIR="$(
    cd "$(dirname "${BASH_SOURCE[0]}")"
    pwd
)"

PROJECT_DIR="$(
    cd "$SCRIPT_DIR/.."
    pwd
)"

DISK_LIMIT="${DISK_LIMIT:-85}"
MEMORY_LIMIT_MB="${MEMORY_LIMIT_MB:-512}"

failed_count=0
if ! [[ "$DISK_LIMIT" =~ ^[0-9]+$ ]]; then
    echo "错误：DISK_LIMIT必须是整数"
    exit 1
fi

if ! [[ "$MEMORY_LIMIT_MB" =~ ^[0-9]+$ ]]; then
    echo "错误：MEMORY_LIMIT_MB必须是整数"
    exit 1
fi

if [ "$DISK_LIMIT" -lt 1 ] || [ "$DISK_LIMIT" -gt 100 ]; then
    echo "错误：DISK_LIMIT必须在1～100之间"
    exit 1
fi

# ==============================
# 2. 日志函数
# ==============================

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}


# ==============================
# 3. 磁盘检查
# ==============================

disk_usage=$(
    df -P "$PROJECT_DIR" |
    awk 'NR == 2 {
        gsub("%", "", $5)
        print $5
    }'
)

if [ "$disk_usage" -ge "$DISK_LIMIT" ]; then
    log "失败：磁盘使用率=${disk_usage}%，阈值=${DISK_LIMIT}%"
    failed_count=$((failed_count + 1))
else
    log "通过：磁盘使用率=${disk_usage}%"
fi


# ==============================
# 4. 可用内存检查
# ==============================

available_memory_kb=$(
    awk '
        /MemAvailable/ {
            print $2
        }
    ' /proc/meminfo
)

available_memory_mb=$((available_memory_kb / 1024))

if [ "$available_memory_mb" -lt "$MEMORY_LIMIT_MB" ]; then
    log "失败：可用内存=${available_memory_mb}MB，最低要求=${MEMORY_LIMIT_MB}MB"
    failed_count=$((failed_count + 1))
else
    log "通过：可用内存=${available_memory_mb}MB"
fi


# ==============================
# 5. CPU信息
# ==============================

cpu_count=$(nproc)

load_one_minute=$(
    awk '{print $1}' /proc/loadavg
)

log "信息：CPU核心数=$cpu_count"
log "信息：最近1分钟平均负载=$load_one_minute"


# ==============================
# 6. 项目空间
# ==============================

project_size=$(
    du -sh "$PROJECT_DIR" |
    awk '{print $1}'
)

log "信息：项目占用空间=$project_size"


# ==============================
# 7. ETL进程检查
# ==============================

etl_processes=$(
    pgrep -af '[S]RC/main.py' || true
)

if [ -n "$etl_processes" ]; then
    log "警告：发现正在运行的Python ETL进程"
    echo "$etl_processes"
else
    log "通过：当前没有Python ETL进程"
fi


# ==============================
# 8. 最终判断
# ==============================

if [ "$failed_count" -gt 0 ]; then
    log "系统健康检查失败，失败项数量=$failed_count"
    exit 1
fi

log "系统健康检查通过"
exit 0
