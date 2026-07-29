#!/usr/bin/env bash

set -euo pipefail

value=${1:-}
if [[ "$value" != "true" && "$value" != "false" ]]; then
  echo "usage: $0 true|false" >&2
  exit 2
fi

cluster_dir=${TIUP_CLUSTER_DIR:-/root/cs/jt_poc/tidb-server-v7.1.9-0.1-linux-arm64}
cluster_name=${TIUP_CLUSTER_NAME:-csultra}
script_path=$(realpath "$0")
editor_path=$(dirname "$script_path")/set-pessimistic-autocommit-editor.py
cd "$cluster_dir"

export PAUTO_VALUE=$value
export EDITOR=$editor_path
./tiup-cluster edit-config "$cluster_name" --yes
./tiup-cluster reload "$cluster_name" -R tidb -y --wait-timeout 300
./tiup-cluster display "$cluster_name"
./tiup-cluster show-config "$cluster_name" |
  grep -F "pessimistic-txn.pessimistic-auto-commit: $value"
