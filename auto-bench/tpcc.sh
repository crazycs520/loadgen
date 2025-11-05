#!/bin/bash

set -ex

MYSQL_HOST="${1:-127.0.0.1}"
MYSQL_PORT="${2:-4000}"
PREPARE_SQL="${3:-prepare.sql}"
LOG_DIR="${4:-logs}"
THREADS_STR="8,16"
WAREHOUSES=1
TIME="1m"

# 将线程数字符串转为数组
IFS=',' read -ra THREADS <<< "$THREADS_STR"
echo "${THREADS[@]}"

mkdir -p "$LOG_DIR"

# 3. TPCC 测试
tiup mirror set https://tiup-mirrors.pingcap.com
tiup bench tpcc prepare --host "$MYSQL_HOST" --port "$MYSQL_PORT" --user root --warehouses "$WAREHOUSES" > "${LOG_DIR}/tpcc_prepare.log" 2>&1

mysql -u root -h "$MYSQL_HOST" -P "$MYSQL_PORT" -e "select tidb_version();"
cat "$PREPARE_SQL"
echo
mysql -u root -h "$MYSQL_HOST" -P "$MYSQL_PORT" < "$PREPARE_SQL"

extract_tpcc_thread_tpmc() {
    set +x

    log_file="$1"

    thread=$(grep -oP 'threads \K[0-9]+' "$log_file" | head -n1)

    tpmC=$(grep -oP 'tpmC: \K[0-9.]+(?=,)' "$log_file" | tail -n1)

    echo "threads: $thread, tpmC: $tpmC"

    set -x
}

# 运行测试
for threads in "${THREADS[@]}"; do
    log_file="${LOG_DIR}/tpcc_${threads}threads.log"
    date >> "$log_file"
    tiup bench tpcc run \
        --host "$MYSQL_HOST" \
        --port "$MYSQL_PORT" \
        --user root \
        --warehouses "$WAREHOUSES" \
        --threads "$threads" \
        --time "$TIME" > "$log_file" 2>&1

        extract_tpcc_thread_tpmc "$log_file"
done
