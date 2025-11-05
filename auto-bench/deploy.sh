#!/bin/bash
set -euo pipefail

# 参数检查
if [[ $# -lt 3 ]]; then
    echo "用法: $0 <cluster_name> <version> <topo.yaml> [patch_binary_path] [ssh-key-file]"
    exit 1
fi

cluster_name=$1
cluster_version=$2
topo_file=$3
patch_binary_path=${4:-}
ssh_key=${5:-~/.ssh/id_ed25519}

# tiup-cluster 路径，按需修改

if [[ $cluster_version != v7.1.8* ]]; then
    tiup mirror set https://tiup-mirrors.pingcap.com
    tiup_bin="tiup cluster"
else
    # todo
    tiup mirror set https://tiup-mirrors.pingcap.com
    tiup_bin="bin/tiup-cluster"
fi

# 部署
echo "==> 部署集群: $cluster_name"
$tiup_bin deploy "$cluster_name" "$cluster_version" "$topo_file" -i "$ssh_key" --yes

# 如果给了 patch 路径，则打 patch
if [[ -n "$patch_binary_path" ]]; then
    echo "==> 打 patch: $patch_binary_path"
    $tiup_bin patch "$cluster_name" "$patch_binary_path" -R tidb --offline --yes
fi

# 启动集群
echo "==> 启动集群: $cluster_name"
$tiup_bin start "$cluster_name"

$tiup_bin display "$cluster_name"

echo "==> 完成 ✅"
