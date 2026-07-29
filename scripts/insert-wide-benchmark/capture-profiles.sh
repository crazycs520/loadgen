#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 CASE_FILE" >&2
  exit 2
fi

case_file=$1
loadgen_bin=${LOADGEN_BIN:-./bin/loadgen}
profile_root=${PROFILE_ROOT:-/root/cs/insert-test/profiles}
tidb_binary=${TIDB_BINARY:-/data0/dbcs4/deploy/tidb-4301/bin/tidb-server}
profile_seconds=${PROFILE_SECONDS:-30}
rows=${ROWS:-1000000}
batch_size=${BATCH_SIZE:-100}
threads=${THREADS:-8}
hosts=${HOSTS:-10.2.13.213}
ports=${PORTS:-4301,4302,4303}
status_ports=${STATUS_PORTS:-13581 13582 13583}
database=${DATABASE:-insert_bench}

mkdir -p "$profile_root"
while IFS=$'\t' read -r case_name protocol txn_mode txn_statements key_mode session_variables; do
  [[ -z "$case_name" || "$case_name" == \#* ]] && continue
  case_dir="$profile_root/$case_name"
  mkdir -p "$case_dir"

  echo "PROFILE_START case=$case_name time=$(date -Iseconds)"
  profile_pids=()
  profile_files=()
  for status_port in $status_ports; do
    profile_file="$case_dir/tidb-${status_port}.pb.gz"
    profile_files+=("$profile_file")
    curl --fail --silent --show-error --max-time "$((profile_seconds + 15))" \
      "http://127.0.0.1:${status_port}/debug/pprof/profile?seconds=${profile_seconds}" \
      --output "$profile_file" &
    profile_pids+=("$!")
  done

  set +e
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
    --result-file="$case_dir/result.json" \
    > "$case_dir/loadgen.log" 2>&1
  loadgen_status=$?
  set -e

  profile_status=0
  for pid in "${profile_pids[@]}"; do
    if ! wait "$pid"; then
      profile_status=1
    fi
  done
  if (( loadgen_status != 0 )); then
    echo "loadgen failed for $case_name" >&2
    exit "$loadgen_status"
  fi
  if (( profile_status != 0 )); then
    echo "profile collection failed for $case_name" >&2
    exit "$profile_status"
  fi

  go tool pprof -proto -output "$case_dir/tidb-merged.pb.gz" \
    "$tidb_binary" "${profile_files[@]}" \
    > "$case_dir/pprof-merge.log" 2>&1
  go tool pprof -top -nodecount=80 \
    "$tidb_binary" "$case_dir/tidb-merged.pb.gz" \
    > "$case_dir/tidb-merged-top.txt" 2>&1
  jq -r '"PROFILE_END case=\(.case_name) duration=\(.duration_seconds)s rows_per_second=\(.rows_per_second)"' \
    "$case_dir/result.json"
  sleep 5
done < "$case_file"

