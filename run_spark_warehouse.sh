#!/usr/bin/env bash

# 管道中任何一个命令失败，都能取得它的状态码
set -o pipefail

# 使用不存在的变量时报错
set -u


# =========================================================
# 1. 取得项目根目录
# =========================================================

# 无论从哪个目录执行脚本，都能找到项目根目录
SCRIPT_DIR="$(
    cd "$(dirname "${BASH_SOURCE[0]}")"
    pwd
)"


# =========================================================
# 2. 程序路径
# =========================================================

PYTHON="$SCRIPT_DIR/.venv/bin/python"
SPARK_SUBMIT="$SCRIPT_DIR/.venv/bin/spark-submit"

SPARK_SCRIPT_DIR="$SCRIPT_DIR/spark_learning"
LOG_DIR="$SCRIPT_DIR/logs"

LOCK_FILE="$SCRIPT_DIR/.spark_warehouse.lock"


# =========================================================
# 3. Spark运行参数
# =========================================================

# 如果外部没有传入参数，就使用冒号后面的默认值
SPARK_MASTER="${SPARK_MASTER:-local[4]}"
DRIVER_MEMORY="${DRIVER_MEMORY:-2g}"
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-8}"

# Java安装目录
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"

# 把Java和Linux命令目录加入PATH
export PATH="$JAVA_HOME/bin:/usr/bin:/bin:${PATH:-/usr/bin:/bin}"

# 指定Spark使用当前项目的虚拟环境Python
export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"


# =========================================================
# 4. 本次运行时间和日志文件
# =========================================================

RUN_TIME="$(date '+%Y%m%d_%H%M%S')"
RUN_LOG="$LOG_DIR/spark_warehouse_$RUN_TIME.log"

PIPELINE_START_TIME="$(date +%s)"


# =========================================================
# 5. Spark任务清单
# ========================================================
SPARK_JOBS=(
    "02_read_raw_csv.py"
    "03_build_ods_order.py"
    "04_build_ods_order_detail.py"
    "05_build_ods_customer.py"
    "06_build_ods_product.py"
    "07_check_ods_relationships.py"
    "08_build_dwd_order_detail.py"
    "09_build_dws_user_sales.py"
    "10_build_dws_product_sales.py"
    "11_build_dws_area_sales.py"
    "12_build_ads_sales_summary.py"
    "13_build_ads_monthly_sales_trend.py"
    "14_build_ads_user_rfm_base.py"
    "15_build_ads_user_rfm_segment.py"
    "16_build_ads_product_abc.py"
    "17_check_spark_warehouse_metrics.py"
)


# =========================================================
# 6. 运行状态变量
# =========================================================

spark_pid=""
current_task="未开始"
success_count=0
total_jobs="${#SPARK_JOBS[@]}"


# =========================================================
# 7. 日志函数
# =========================================================

log() {
    local message="$1"

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $message" |
        tee -a "$RUN_LOG"
}


# =========================================================
# 8. 脚本退出处理
# =========================================================

on_exit() {
    local exit_status=$?
    local pipeline_end_time
    local elapsed_seconds

    pipeline_end_time="$(date +%s)"

    # 注意：Bash算术计算必须写在 $((...)) 里面
    elapsed_seconds=$((pipeline_end_time - PIPELINE_START_TIME))

    if [ "$exit_status" -eq 0 ]; then
        log "Spark数仓流水线正常结束"
        log "成功任务数：$success_count/$total_jobs"
    else
        log "Spark数仓流水线异常结束"
        log "失败任务：$current_task"
    fi

    log "最终状态码：$exit_status"
    log "总耗时：${elapsed_seconds}秒"
    log "完整日志：$RUN_LOG"
}


# =========================================================
# 9. 系统信号处理
# =========================================================

on_signal() {
    local signal_name="$1"
    local exit_status="$2"

    log "收到系统信号：$signal_name"
    log "准备停止Spark流水线"

    if [ -n "$spark_pid" ] &&
        kill -0 "$spark_pid" 2>/dev/null
    then
        log "正在终止Spark子进程：$spark_pid"

        kill "$spark_pid" 2>/dev/null

        # 等待子进程真正退出
        wait "$spark_pid" 2>/dev/null
    fi

    exit "$exit_status"
}


# =========================================================
# 10. 检查整数参数
# =========================================================

check_integer() {
    local parameter_name="$1"
    local parameter_value="$2"

    if ! [[ "$parameter_value" =~ ^[1-9][0-9]*$ ]]; then
        log "失败：$parameter_name 必须是大于0的整数"
        log "当前值：$parameter_value"

        return 1
    fi

    return 0
}


# =========================================================
# 11. 检查程序文件
# =========================================================

check_programs() {
    if [ ! -x "$PYTHON" ]; then
        log "失败：虚拟环境Python不存在或不可执行"
        log "文件：$PYTHON"

        return 1
    fi

    if [ ! -x "$SPARK_SUBMIT" ]; then
        log "失败：spark-submit不存在或不可执行"
        log "文件：$SPARK_SUBMIT"

        return 1
    fi

    if [ ! -x "$JAVA_HOME/bin/java" ]; then
        log "失败：Java不存在或不可执行"
        log "文件：$JAVA_HOME/bin/java"

        return 1
    fi

    if [ ! -d "$SPARK_SCRIPT_DIR" ]; then
        log "失败：Spark学习目录不存在"
        log "目录：$SPARK_SCRIPT_DIR"

        return 1
    fi

    return 0
}


# =========================================================
# 12. 检查全部Spark脚本
# =========================================================

check_spark_scripts() {
    local job_name
    local job_file
    local failed_count=0

    log "开始检查全部Python脚本语法"

    for job_name in "${SPARK_JOBS[@]}"
    do
        job_file="$SPARK_SCRIPT_DIR/$job_name"

        # 第一步：检查文件是否存在
        if [ ! -f "$job_file" ]; then
            log "失败：Spark脚本不存在"
            log "文件：$job_file"

            failed_count=$((failed_count + 1))

            continue
        fi

        # 第二步：检查Python语法
        if "$PYTHON" -m py_compile "$job_file"
        then
            log "语法通过：$job_name"
        else
            log "语法失败：$job_name"

            failed_count=$((failed_count + 1))
        fi
    done

    if [ "$failed_count" -gt 0 ]; then
        log "Spark脚本检查失败，失败数量：$failed_count"

        return 1
    fi

    log "全部Spark脚本检查通过"

    return 0
}


# =========================================================
# 13. 运行单个Spark任务
# =========================================================

run_spark_job() {
    local job_name="$1"
    local job_file="$SPARK_SCRIPT_DIR/$job_name"

    local job_start_time
    local job_end_time
    local elapsed_seconds
    local job_status

    current_task="$job_name"
    job_start_time="$(date +%s)"

    log "----------------------------------------"
    log "开始执行任务：$job_name"

    "$SPARK_SUBMIT" \
        --master "$SPARK_MASTER" \
        --driver-memory "$DRIVER_MEMORY" \
        --conf "spark.sql.shuffle.partitions=$SHUFFLE_PARTITIONS" \
        "$job_file" \
        > >(tee -a "$RUN_LOG") \
        2>&1 &

    # $!表示刚刚放到后台运行的进程编号
    spark_pid=$!

    log "Spark子进程PID：$spark_pid"

    # 等待这个Spark任务执行完成
    wait "$spark_pid"
    job_status=$?

    spark_pid=""

    job_end_time="$(date +%s)"
    elapsed_seconds=$((job_end_time - job_start_time))

    if [ "$job_status" -ne 0 ]; then
        log "任务执行失败：$job_name"
        log "任务状态码：$job_status"
        log "任务耗时：${elapsed_seconds}秒"

        return "$job_status"
    fi

    success_count=$((success_count + 1))

    log "任务执行成功：$job_name"
    log "任务耗时：${elapsed_seconds}秒"
    log "任务进度：$success_count/$total_jobs"

    return 0
}


# =========================================================
# 14. 初始化日志和退出监听
# =========================================================

mkdir -p "$LOG_DIR"

trap on_exit EXIT
trap 'on_signal "SIGINT" 130' INT
trap 'on_signal "SIGTERM" 143' TERM


# =========================================================
# 15. 防止流水线重复运行
# =========================================================

exec 9>"$LOCK_FILE"

if ! flock -n 9
then
    current_task="任务锁检查"

    log "Spark数仓流水线已经在运行"
    log "本次任务不再重复启动"

    exit 2
fi


# =========================================================
# 16. 输出运行环境
# =========================================================

log "========================================"
log "Spark数仓流水线开始"
log "项目目录：$SCRIPT_DIR"
log "运行日志：$RUN_LOG"
log "Spark Master：$SPARK_MASTER"
log "Driver内存：$DRIVER_MEMORY"
log "Shuffle分区：$SHUFFLE_PARTITIONS"


# =========================================================
# 17. 检查参数
# =========================================================

current_task="参数检查"

if ! check_integer \
    "SHUFFLE_PARTITIONS" \
    "$SHUFFLE_PARTITIONS"
then
    exit 1
fi


# =========================================================
# 18. 检查运行环境
# =========================================================

current_task="运行环境检查"

if ! check_programs
then
    exit 1
fi

python_version="$("$PYTHON" --version 2>&1)"

spark_version="$(
    "$PYTHON" -c \
        "import pyspark; print(pyspark.__version__)"
)"

java_version="$(
    "$JAVA_HOME/bin/java" -version 2>&1 |
        head -n 1
)"

log "Python版本：$python_version"
log "Spark版本：$spark_version"
log "Java版本：$java_version"


# =========================================================
# 19. 检查Spark脚本
# =========================================================

current_task="脚本检查"

if ! check_spark_scripts
then
    exit 1
fi


# =========================================================
# 20. 按顺序执行全部任务
# =========================================================

for job_name in "${SPARK_JOBS[@]}"
do
    run_spark_job "$job_name"
    job_status=$?

    if [ "$job_status" -ne 0 ]; then
        log "流水线停止，不再执行后续任务"

        exit "$job_status"
    fi
done


# =========================================================
# 21. 全部任务完成
# =========================================================

current_task="全部完成"

log "----------------------------------------"
log "全部Spark数仓任务执行完成"
log "成功任务数：$success_count/$total_jobs"
log "========================================"

exit 0
