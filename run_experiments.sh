#!/bin/bash

# 配置参数
JAR_FILE="target/GETL-1.00.jar"
JVM_MEMORY="-Xms9g -Xmx9g -Xss64m"
SLEEP_TIME=5
FILE_SUFFIX="$(date '+%Y%m%d_%H%M%S')"
LOG_DIR=".logs_${FILE_SUFFIX}"
# 创建日志目录
mkdir -p "${LOG_DIR}"
echo "日志目录: ${LOG_DIR}"

# 查询列表
QUERIES=("q1" "q2" "q3" "q4" "q5" "q6" "q7")
# 模型转换任务
CONVERSIONS=("lpg" "rdf" "rm")
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
    local start_time=$(date +%s)

    log "开始执行: -c ${class} -m ${model}"

    # 执行 Java 程序，每次启动新的 JVM 实例
    java ${JVM_MEMORY} -jar ${JAR_FILE} -c ${class} -m ${model} -s ${suffix} > ${log_file} 2>&1

    local exit_code=$?
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    local formatted_duration=$(format_duration $duration)

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
    local script_start_time=$(date +%s)

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

    local script_end_time=$(date +%s)
    local total_duration=$((script_end_time - script_start_time))
    local formatted_total_duration=$(format_duration $total_duration)

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
