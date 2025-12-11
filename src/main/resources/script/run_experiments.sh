#!/bin/bash

# 配置参数
JAR_FILE="target/GETL-1.00.jar"
JVM_MEMORY="-Xms650g -Xmx650g -Xss64m"
SLEEP_TIME=5
FILE_SUFFIX="$(date '+%Y%m%d_%H%M%S')"
LOG_DIR=".logs_${FILE_SUFFIX}"
# 创建日志目录
mkdir -p "${LOG_DIR}"
echo "日志目录: ${LOG_DIR}"

# 检查 CONFIG_PATH 环境变量是否存在
if [ -z "${CONFIG_PATH}" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: CONFIG_PATH 环境变量未设置。请设置 CONFIG_PATH 后重试。"
    exit 1
fi

# 查询列表
QUERIES=("q1" "q5")
# 模型转换任务
CONVERSIONS=("lpg" "rdf" "rm" "RDF2LPG" "RM2LPG")
# 模型类型
MODELS=("ug" "mg" "sg")

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# 格式化时间函数
format_duration() {
    local duration=$1
    local hours=$((duration / 3600))
    local minutes=$(((duration % 3600) / 60))
    local seconds=$((duration % 60))
    printf "%02d:%02d:%02d" $hours $minutes $seconds
}

# 执行任务函数
run_task() {
    local class=$1
    local model=$2
    local suffix=${FILE_SUFFIX}
    local log_file="${LOG_DIR}/${class}_${model}.log"
    local start_time
    start_time=$(date +%s)

    log "开始执行: -c ${class} -m ${model}"

    # 启动 Java 程序到后台，捕获 PID 并重定向输出到日志文件
    java ${JVM_MEMORY} -jar ${JAR_FILE} -c ${class} -m ${model} -s ${suffix} > "${log_file}" 2>&1 &
    local pid=$!
#    log "启动 JVM (pid=${pid})，日志: ${log_file}"

    # 等待 JVM 退出并获取退出码
    wait ${pid}
    local exit_code=$?
#    log "JVM (pid=${pid}) 已退出，退出码: ${exit_code}"

    # 确认没有残留与本次运行相同参数的 java 进程
    local jar_basename
    jar_basename=$(basename "${JAR_FILE}")
    local max_wait=120   # 最长等待秒数，可根据需要调整
    local waited=0
    local sleep_interval=1
    local pids=()

    # 使用 pgrep -f 精确匹配命令行（jar basename 和 -s suffix），并仅匹配当前用户的进程
    # pgrep 在 macOS 和 Linux 上都可用，-f 匹配完整命令行
    while [ ${waited} -lt ${max_wait} ]; do
        # 获取匹配的 PID 列表
        # 用 printf 将结果填入数组以处理无匹配的情况
        IFS=$'\n' read -r -d '' -a pids < <(pgrep -u "$(whoami)" -f "${jar_basename}.*-s ${suffix}" 2>/dev/null || true; printf '\0')

        # 过滤掉空条目
        if [ ${#pids[@]} -eq 0 ]; then
            break
        fi

        # 如果只有已经退出的 pid，数组会为空；否则等待
        log "检测到残留 JVM PID(s): ${pids[*]}，等待其退出... (waited=${waited}s)"
        sleep ${sleep_interval}
        waited=$((waited + sleep_interval))
    done

    # 再次收集 pids，准备进行终止（如果有的话）
    IFS=$'\n' read -r -d '' -a pids < <(pgrep -u "$(whoami)" -f "${jar_basename}.*-s ${suffix}" 2>/dev/null || true; printf '\0')

    if [ ${#pids[@]} -ne 0 ]; then
        log "残留 JVM 在 ${max_wait}s 后仍未退出，尝试优雅终止 PID(s): ${pids[*]}"
        # 发送 TERM
        printf '%s\n' "${pids[@]}" | xargs -r kill -TERM
        sleep 5
        # 再检查并强制杀死剩余的
        IFS=$'\n' read -r -d '' -a pids < <(pgrep -u "$(whoami)" -f "${jar_basename}.*-s ${suffix}" 2>/dev/null || true; printf '\0')
        if [ ${#pids[@]} -ne 0 ]; then
            log "进程仍然存在，强制杀死 PID(s): ${pids[*]}"
            printf '%s\n' "${pids[@]}" | xargs -r kill -KILL
        fi
        log "已对残留 JVM 发送终止信号。"
    fi

    local end_time
    end_time=$(date +%s)
    local duration
    duration=$((end_time - start_time))
    local formatted_duration
    formatted_duration=$(format_duration $duration)

    if [ ${exit_code} -eq 0 ]; then
        log "成功完成: -c ${class} -m ${model}, 耗时: ${formatted_duration}"
    else
        log "执行失败: -c ${class} -m ${model}, 退出码: ${exit_code}, 耗时: ${formatted_duration}"
    fi

    # 等待一段时间,确保 JVM 完全退出并释放内存
    sleep ${SLEEP_TIME}

    return ${exit_code}
}

# 主执行流程
main() {
    local script_start_time
    script_start_time=$(date +%s)

    log "============================================"
    log "开始矩阵化执行实验"
    log "============================================"

    local total=0
    local success=0
    local failed=0

    # 执行所有查询任务
    for query in "${QUERIES[@]}"; do
        for model in "${MODELS[@]}"; do
            run_task ${query} ${model}
            total=$((total + 1))
            if [ $? -eq 0 ]; then
                success=$((success + 1))
            else
                failed=$((failed + 1))
            fi
        done
    done

    # 执行所有模型转换任务
    for conversion in "${CONVERSIONS[@]}"; do
        for model in "${MODELS[@]}"; do
            run_task ${conversion} ${model}
            exit_code=$?
            total=$((total + 1))
            if [ $exit_code -eq 0 ]; then
                success=$((success + 1))
            else
                failed=$((failed + 1))
            fi
        done
    done

    local script_end_time
    script_end_time=$(date +%s)
    local total_duration
    total_duration=$((script_end_time - script_start_time))
    local formatted_total_duration
    formatted_total_duration=$(format_duration $total_duration)

    # 输出执行统计
    log "============================================"
    log "执行完成"
    log "总任务数: ${total}"
    log "成功: ${success}"
    log "失败: ${failed}"
    log "总耗时: ${formatted_total_duration}"
    log "日志目录: ${LOG_DIR}"
    log "============================================"
}

# 执行主函数
main
