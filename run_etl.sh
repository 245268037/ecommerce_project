#!/bin/bash

# 管道中的任何命令失败，都能被检测到
set -o pipefail


# ==============================
# 1. 项目路径
# ==============================

SCRIPT_DIR="$(
    cd "$(dirname "${BASH_SOURCE[0]}")"
    pwd
)"

PYTHON="$SCRIPT_DIR/.venv/bin/python"
MAIN_FILE="$SCRIPT_DIR/SRC/main.py"
RAW_DIR="$SCRIPT_DIR/data/raw"
LOG_DIR="$SCRIPT_DIR/logs"
HEALTH_CHECK="$SCRIPT_DIR/scripts/system_health_check.sh"
OUTPUT_CHECK="$SCRIPT_DIR/scripts/check_etl_output.sh"
METRIC_CHECK="$SCRIPT_DIR/SRC/check_warehouse_metrics.py"

if [ ! -f "$METRIC_CHECK" ]; then
    log "失败：业务指标检查程序不存在：$METRIC_CHECK"
    exit 1
fi
if [ ! -x "$OUTPUT_CHECK" ]; then
    log "失败：输出检查脚本不存在或不可执行：$OUTPUT_CHECK"
    exit 1
fi

RUN_TIME=$(date '+%Y%m%d_%H%M%S')
RUN_LOG="$LOG_DIR/etl_run_$RUN_TIME.log"
ETL_START_EPOCH=$(date +%s)
python_pid=""

# ==============================
# 2. 输入文件数组
# ==============================

INPUT_FILES=(
    "order.csv"
    "order_detail.csv"
    "customer.csv"
    "product.csv"
)


# ==============================
# 3. 日志函数
# ==============================

log() {
    local message="$1"

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $message" \
        | tee -a "$RUN_LOG"
}
# ==============================
# 退出处理
# ==============================

on_exit() {
    local exit_status=$?

    if [ "$exit_status" -eq 0 ]; then
        log "启动脚本结束，最终状态码=0"
    else
        log "启动脚本异常结束，最终状态码=$exit_status"
    fi
}


# ==============================
# 信号处理
# ==============================
on_signal() {
    local signal_name="$1"
    local exit_status="$2"

    log "收到系统信号：$signal_name，准备终止任务"

    if [ -n "$python_pid" ] &&
       kill -0 "$python_pid" 2>/dev/null
    then
        log "正在终止Python子进程：$python_pid"

        kill "$python_pid"

        wait "$python_pid" 2>/dev/null
    fi

    exit "$exit_status"
}


# ==============================
# 4. 文件检查函数
# ==============================

check_file() {
    local file_path="$1"

    if [ ! -f "$file_path" ]; then
        log "失败：文件不存在：$file_path"
        return 1
    fi

    if [ ! -r "$file_path" ]; then
        log "失败：文件不可读：$file_path"
        return 1
    fi

    if [ ! -s "$file_path" ]; then
        log "失败：文件为空：$file_path"
        return 1
    fi

    local data_rows

    data_rows=$(
        tail -n +2 "$file_path" |
        wc -l
    )

    log "通过：$(basename "$file_path")，数据行数=$data_rows"

    return 0
}


# ==============================
# 5. 初始化日志目录
# ==============================

mkdir -p "$LOG_DIR"
trap on_exit EXIT
trap 'on_signal "SIGINT" 130' INT
trap 'on_signal "SIGTERM" 143' TERM

# ==============================
# 系统健康检查
# ==============================

if [ ! -x "$HEALTH_CHECK" ]; then
    log "失败：健康检查脚本不存在或不可执行：$HEALTH_CHECK"
    exit 1
fi

log "开始执行系统健康检查"

"$HEALTH_CHECK" 2>&1 |
    tee -a "$RUN_LOG"

health_status=${PIPESTATUS[0]}

if [ "$health_status" -ne 0 ]; then
    log "系统健康检查失败，ETL任务终止"
    exit 1
fi

log "系统健康检查通过，允许启动ETL"

# ==============================
# 防止ETL任务重复运行
# ==============================

LOCK_FILE="$SCRIPT_DIR/.etl.lock"

exec 9>"$LOCK_FILE"

if ! flock -n 9; then
    log "ETL任务已经在运行，本次任务不再重复启动"
    exit 2
fi

log "========================================"
log "电商ETL任务开始"
log "项目目录：$SCRIPT_DIR"
log "运行日志：$RUN_LOG"


# ==============================
# 6. 检查Python环境
# ==============================

if [ ! -x "$PYTHON" ]; then
    log "失败：虚拟环境Python不存在或不可执行"
    log "Python路径：$PYTHON"
    exit 1
fi

log "Python环境：$("$PYTHON" --version 2>&1)"


# ==============================
# 7. 检查主程序
# ==============================

if [ ! -f "$MAIN_FILE" ]; then
    log "失败：找不到主程序：$MAIN_FILE"
    exit 1
fi


# ==============================
# 8. 批量检查输入文件
# ==============================

failed_count=0

for file_name in "${INPUT_FILES[@]}"
do
    file_path="$RAW_DIR/$file_name"

    if ! check_file "$file_path"; then
        failed_count=$((failed_count + 1))
    fi
done

if [ "$failed_count" -gt 0 ]; then
    log "输入文件检查失败，异常文件数：$failed_count"
    log "ETL任务终止"
    exit 1
fi

log "全部输入文件检查通过"


# ==============================
# 9. 执行Python ETL
# ==============================

log "开始执行Python主程序"

cd "$SCRIPT_DIR/SRC" || {
    log "失败：无法进入SRC目录"
    exit 1
}

"$PYTHON" "$MAIN_FILE" \
    > >(tee -a "$RUN_LOG") \
    2>&1 &

python_pid=$!

log "Python子进程PID：$python_pid"

wait "$python_pid"
python_status=$?

python_pid=""


# ==============================
# 10. 判断运行结果
# ==============================

if [ "$python_status" -ne 0 ]; then
    log "ETL执行失败，Python状态码：$python_status"
    exit "$python_status"
fi

# ==============================
# 10. ETL输出文件检查
# ==============================

log "开始执行ETL输出文件检查"

MIN_MTIME_EPOCH="$ETL_START_EPOCH" \
    "$OUTPUT_CHECK" 2>&1 |
    tee -a "$RUN_LOG"

output_check_status=${PIPESTATUS[0]}

if [ "$output_check_status" -ne 0 ]; then
    log "ETL输出文件检查失败，状态码：$output_check_status"
    exit "$output_check_status"
fi

log "ETL输出文件检查通过"

# ==============================
# 业务指标一致性检查
# ==============================

log "开始执行数仓业务指标检查"

PYTHONPATH="$SCRIPT_DIR/SRC" \
    "$PYTHON" "$METRIC_CHECK" 2>&1 |
    tee -a "$RUN_LOG"

metric_check_status=${PIPESTATUS[0]}

if [ "$metric_check_status" -ne 0 ]; then
    log "数仓业务指标检查失败，状态码：$metric_check_status"
    exit "$metric_check_status"
fi

log "数仓业务指标检查通过"

log "ETL执行成功"
log "任务日志：$RUN_LOG"
log "========================================"

exit 0
