#!/bin/bash

set -ex


MYSQL_HOST="${1:-127.0.0.1}"
MYSQL_PORT="${2:-4000}"
PREPARE_SQL="${3:-prepare.sql}"
LOG_DIR="${4:-logs}"
PREPARE_DATA="${5:-1}"
TABLES=32
TABLE_SIZE=10000
THREADS_STR="16,32,64"
TIME=20


# 将线程数字符串转为数组
IFS=',' read -ra THREADS <<< "$THREADS_STR"
echo "${THREADS[@]}"

workloads=("oltp_point_select" "oltp_batch_point_select" "oltp_read_only" "oltp_read_write")

# sysbench 测试
mkdir -p "$LOG_DIR"


if [[ $PREPARE_DATA == "1" ]]; then
    # mysql -u root -h "$MYSQL_HOST" -P "$MYSQL_PORT" -e "drop database if exists test;"
    # mysql -u root -h "$MYSQL_HOST" -P "$MYSQL_PORT" -e "create database test;"
    sysbench oltp_point_select \
        --db-driver=mysql \
        --mysql-host="$MYSQL_HOST" \
        --mysql-port="$MYSQL_PORT" \
        --mysql-user=root \
        --mysql-password="" \
        --mysql-db="test" \
        --tables="$TABLES" \
        --table-size="$TABLE_SIZE" \
        --threads=32 \
        prepare > "${LOG_DIR}/sysbench_prepare.log" 2>&1
fi

mysql -u root -h "$MYSQL_HOST" -P "$MYSQL_PORT" -e "select tidb_version();"
cat "$PREPARE_SQL"
echo
mysql -u root -h "$MYSQL_HOST" -P "$MYSQL_PORT" < "$PREPARE_SQL"

extract_sysbench_metrics() {
    set +x

    log_file="$1"
    workload="$2"

    if [ ! -f "$log_file" ]; then
        echo "❌ File not found: $log_file" >&2
        return 1
    fi

    thread=$(grep -E "Number of threads:" "$log_file" | grep -Eo '[0-9]+')
    tps=$(grep -E "transactions:" "$log_file" | awk -F'[()]' '{print $2}' | awk '{print $1}')
    qps=$(grep -E "queries:" "$log_file" | grep -v "performed" | awk -F'[()]' '{print $2}' | awk '{print $1}')
    avg_latency=$(grep -E "avg:" "$log_file" | awk '{print $2}')
    p95_latency=$(grep -E "95th percentile:" "$log_file" | awk '{print $3}')

    echo "$workload threads: $thread, tps: $tps, qps: $qps, avg_duration: $avg_latency ms, p95_duration: $p95_latency ms"

    set -x
}

# 运行多种 workload 测试

for workload in "${workloads[@]}"; do
    for threads in "${THREADS[@]}"; do
        log_file="${LOG_DIR}/sysbench_${workload}_${threads}threads.log"
        date >> "$log_file"
        sysbench "$workload" \
            --db-driver=mysql \
            --mysql-host="$MYSQL_HOST" \
            --mysql-port="$MYSQL_PORT" \
            --mysql-user=root \
            --mysql-password="" \
            --report-interval=10 \
            --mysql-db="test" \
            --tables="$TABLES" \
            --table-size="$TABLE_SIZE" \
            --threads="$threads" \
            --time="$TIME" \
            --rand-type=uniform \
            run > "$log_file" 2>&1

            extract_sysbench_metrics "$log_file" "$workload"
    done
done

function exec_sql() {
    mysql -u root -h "$MYSQL_HOST" -P "$MYSQL_PORT" --password="" -e "$1"
}
