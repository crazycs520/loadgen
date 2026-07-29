#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 CASE_FILE" >&2
  exit 2
fi

case_file=$1
loadgen_bin=${LOADGEN_BIN:-./bin/loadgen}
result_root=${RESULT_ROOT:-/root/cs/insert-test/results}
rows=${ROWS:-1000000}
batch_size=${BATCH_SIZE:-100}
threads=${THREADS:-8}
repeats=${REPEATS:-3}
cooldown_seconds=${COOLDOWN_SECONDS:-5}
hosts=${HOSTS:-10.2.13.213}
ports=${PORTS:-4301,4302,4303}
database=${DATABASE:-insert_bench}
mysql_host=${MYSQL_HOST:-127.0.0.1}
mysql_port=${MYSQL_PORT:-4301}

if [[ ! -x "$loadgen_bin" ]]; then
  echo "loadgen binary is not executable: $loadgen_bin" >&2
  exit 2
fi
if [[ ! -f "$case_file" ]]; then
  echo "case file does not exist: $case_file" >&2
  exit 2
fi
if (( rows <= 0 || rows % batch_size != 0 )); then
  echo "ROWS must be positive and divisible by BATCH_SIZE" >&2
  exit 2
fi

mkdir -p "$result_root"
manifest="$result_root/manifest.tsv"
if [[ ! -f "$manifest" ]]; then
  printf 'case\trepetition\tstarted_at\tfinished_at\tduration_seconds\trows\trows_per_second\tprotocol\ttxn_mode\ttxn_statements\tkey_mode\tsession_variables\n' > "$manifest"
fi

mapfile -t cases < <(awk -F '\t' 'NF && $1 !~ /^#/ {print}' "$case_file")
if (( ${#cases[@]} == 0 )); then
  echo "case file has no cases: $case_file" >&2
  exit 2
fi

git_revision=$(git rev-parse HEAD)
cluster_version=$(mysql -h "$mysql_host" -P "$mysql_port" -uroot -NBe 'SELECT VERSION()')
printf 'git_revision=%s\ncluster_version=%s\nrows=%s\nbatch_size=%s\nthreads=%s\nrepeats=%s\nhosts=%s\nports=%s\n' \
  "$git_revision" "$cluster_version" "$rows" "$batch_size" "$threads" "$repeats" "$hosts" "$ports" \
  > "$result_root/run-environment.txt"

for (( repetition = 1; repetition <= repeats; repetition++ )); do
  order_file="$result_root/order-repetition-${repetition}.tsv"
  printf '%s\n' "${cases[@]}" | shuf > "$order_file"

  while IFS=$'\t' read -r case_name protocol txn_mode txn_statements key_mode session_variables; do
    case_dir="$result_root/$case_name"
    result_file="$case_dir/repetition-${repetition}.json"
    log_file="$case_dir/repetition-${repetition}.log"
    mkdir -p "$case_dir"

    if [[ -s "$result_file" ]] && jq -e --argjson rows "$rows" '.rows == $rows and (.error | not)' "$result_file" >/dev/null; then
      echo "SKIP completed case=$case_name repetition=$repetition"
      continue
    fi

    started_at=$(date -Iseconds)
    echo "RUN_START case=$case_name repetition=$repetition time=$started_at"
    "$loadgen_bin" payload insert-wide-benchmark \
      --hosts="$hosts" \
      --ports="$ports" \
      --db="$database" \
      --thread="$threads" \
      --session-variables="$session_variables" \
      --rows="$rows" \
      --batch-size="$batch_size" \
      --txn-mode="$txn_mode" \
      --txn-statements="$txn_statements" \
      --protocol="$protocol" \
      --key-mode="$key_mode" \
      --case-name="$case_name" \
      --result-file="$result_file" \
      > "$log_file" 2>&1

    actual_rows=$(mysql -h "$mysql_host" -P "$mysql_port" -uroot -NBe \
      "SELECT COUNT(*) FROM \`$database\`.\`t_insert_wide\`")
    if [[ "$actual_rows" != "$rows" ]]; then
      echo "row count mismatch for $case_name repetition $repetition: got $actual_rows, want $rows" >&2
      exit 1
    fi

    finished_at=$(date -Iseconds)
    jq -r --arg case_name "$case_name" \
      --arg repetition "$repetition" \
      --arg started_at "$started_at" \
      --arg finished_at "$finished_at" \
      '[
        $case_name,
        $repetition,
        $started_at,
        $finished_at,
        .duration_seconds,
        .rows,
        .rows_per_second,
        .protocol,
        .txn_mode,
        .txn_statements,
        .key_mode,
        .session_variables
      ] | @tsv' "$result_file" >> "$manifest"
    jq -r '"RUN_END case=\(.case_name) duration=\(.duration_seconds)s rows_per_second=\(.rows_per_second)"' "$result_file"
    sleep "$cooldown_seconds"
  done < "$order_file"
done

