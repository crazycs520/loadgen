#!/usr/bin/env bash

set -euo pipefail

if [[ ${1:-} == "--edit" ]]; then
  config_file=${2:?missing TiUP edit-config file}
  value=${PAUTO_VALUE:?missing PAUTO_VALUE}
  python3 - "$config_file" "$value" <<'PY'
import re
import sys

path, value = sys.argv[1], sys.argv[2]
with open(path, encoding="utf-8") as f:
    lines = f.readlines()

key = "pessimistic-txn.pessimistic-auto-commit"
replacement = f"        {key}: {value}\n"
for index, line in enumerate(lines):
    if re.match(rf"^\s+{re.escape(key)}\s*:", line):
        lines[index] = replacement
        break
else:
    for index, line in enumerate(lines):
        if re.match(r"^\s{4}tidb:\s*$", line):
            lines.insert(index + 1, replacement)
            break
    else:
        raise SystemExit("server_configs.tidb was not found")

with open(path, "w", encoding="utf-8") as f:
    f.writelines(lines)
PY
  exit 0
fi

value=${1:-}
if [[ "$value" != "true" && "$value" != "false" ]]; then
  echo "usage: $0 true|false" >&2
  exit 2
fi

cluster_dir=${TIUP_CLUSTER_DIR:-/root/cs/jt_poc/tidb-server-v7.1.9-0.1-linux-arm64}
cluster_name=${TIUP_CLUSTER_NAME:-csultra}
script_path=$(realpath "$0")
cd "$cluster_dir"

export PAUTO_VALUE=$value
export EDITOR="$script_path --edit"
./tiup-cluster edit-config "$cluster_name"
./tiup-cluster reload "$cluster_name" -R tidb -y --wait-timeout 300
./tiup-cluster display "$cluster_name"
./tiup-cluster show-config "$cluster_name" |
  grep -F "pessimistic-txn.pessimistic-auto-commit: $value"
