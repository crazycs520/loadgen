# TiDB v7.1.9-0.1 宽表批量 INSERT 性能测试报告

测试日期：2026-07-29

文档导航：[最佳实践](best-practices.md)

## 1. 结论摘要

在本报告固定的 50 列、2 个唯一索引、2 个普通索引、每条 `INSERT` 写 100 行的场景中：

- 最佳的通用组合是：8 个并发写入连接、服务端 prepare 协议、显式 optimistic 事务、每 50 条 `INSERT` 提交一次，即每事务 5,000 行。
- 该组合写入 1,000,000 行的中位耗时为 **41.843 秒**，相对服务端 prepare + autocommit 的 49.167 秒缩短 **14.9%**。
- 相同事务边界下，optimistic 与 pessimistic 差距很小：50 条/事务时分别为 41.843 秒和 42.218 秒，optimistic 快 0.9%。无写冲突的纯导入应优先 optimistic。
- 显式事务只包 1 条 `INSERT` 没有收益，反而比 autocommit 慢 9.4%～11.6%。收益来自在一个事务中合并多条语句，而不是显式 `BEGIN/COMMIT` 本身。
- 服务端 prepare 在两种事务边界下都最快。autocommit 下比文本协议缩短 10.8%，比客户端参数插值缩短 14.9%；50 条/事务下分别缩短 8.3% 和 13.3%。
- `tidb_constraint_check_in_place=ON` 使 optimistic 50 条/事务的耗时从 41.843 秒增至 134.306 秒，慢 221.0%。`tidb_constraint_check_in_place_pessimistic=ON` 使 pessimistic 对应耗时增加 58.7%。本场景应使用延迟唯一性检查。
- Async Commit 和 1PC 同时开启的耗时为 41.413 秒，只比同时关闭快 1.0%，低于本轮测试可可靠区分的幅度。Prometheus 中两种协议的成功计数均为 0，而普通提交计数持续增加，说明这批跨多 key/Region 的宽表事务实际没有使用它们。
- 在本机同机部署、8 并发条件下，单调主键比打散主键快 6.5%。这是局部性收益大于热点代价的特定结果，不应外推到多机高并发生产环境。

## 2. 测试目标与口径

评价指标只有一个主指标：从表创建完成后开始，到所有行提交完成为止，写入 1,000,000 行的总耗时。

固定条件：

- 总行数：1,000,000。
- 每条 `INSERT`：100 行、50 列，即 5,000 个参数。
- SQL 语句数：10,000。
- 并发连接数：8。
- 写入端点：3 个 TiDB 实例轮询分配给 8 个长连接。
- 每个 case 默认重复 3 次，使用中位数；首次 3 次的最大最小耗时差超过中位数 5% 时补到 5 次。
- 每次测试前 `DROP TABLE` 并重新建表。DDL 不计时；连接、session 初始化、prepare、全部写入及最终 commit 计时；测试后的 `COUNT(*)` 校验不计时。
- 每个正式结果都验证表中恰有 1,000,000 行。
- 同一测试组内，case 顺序在每轮随机打乱；case 之间冷却 5 秒。

共完成 20 个正式 case、64 次百万行写入。`optimistic_10_prepare_default` 和 `pessimistic_10_prepare_default` 因首次波动超过 5% 补跑到 5 次；补跑后的整体波动仍分别为 8.37% 和 5.11%，解读时应降低置信度。其余 case 的波动均低于 5%。

## 3. 环境

### 3.1 集群

| 项目 | 配置 |
|---|---|
| 机器 | `10.2.13.213`，Kunpeng 920，ARM64，96 核，381 GiB，无 swap |
| TiDB | 3 实例，端口 4301～4303，每实例限制 24 CPU / 64 GiB |
| TiKV | 3 实例，端口 20361～20363，每实例限制 24 CPU / 64 GiB |
| PD | 3 实例 |
| Prometheus / Grafana | 9393 / 3303 |
| SQL 版本 | `8.0.11-TiDB-v7.1.9-0.1` |
| TiDB Git | `8a7fa9a85b986748a02025160a561a98ddde478c`，tag `v7.1.9-0.1` |
| TiKV Git | `3ac0c42bc15d8eb2fa4bef3892acd8eec51c2365` |
| PD Git | `40fb236c9bc45360b4b404f453b0c20afa0220c0` |

所有数据库组件与负载生成器都在一台机器上，因此结果适合比较本轮配置的相对差异，不代表多机生产集群的绝对吞吐上限。

正式 case 都显式设置相关 session 变量，避免依赖全局默认值。测试期间将 `tidb_gc_life_time` 从 `10m0s` 临时调到 `24h0m0s`，避免反复删表触发的 GC/compaction 干扰相邻 case；测试完成后恢复。

### 3.2 表结构

表含 50 列，覆盖以下类型：

- 有符号/无符号整数、`DECIMAL`、`FLOAT`、`DOUBLE`、`BOOLEAN`、`BIT`；
- `CHAR`、`VARCHAR`、`TEXT`、`BINARY`、`VARBINARY`；
- `DATE`、`TIME`、`YEAR`、`DATETIME`、`TIMESTAMP`；
- `JSON`、`ENUM`、`SET`。

索引：

- `PRIMARY KEY (id) CLUSTERED`；
- `UNIQUE KEY uk_tenant_order (tenant_id, order_no)`；
- `UNIQUE KEY uk_email (email)`；
- `KEY idx_created_status (created_at, status)`；
- `KEY idx_category_score (category_id, score)`。

默认主键使用 `bits.Reverse64(sequence) >> 1` 确定性打散；单调键对照组使用连续递增值。所有唯一键数据均不重复。

## 4. 并发度与数据量校准

并发度校准使用 200,000 行、autocommit、服务端 prepare：

| 并发 | 耗时（秒） | 行/秒 |
|---:|---:|---:|
| 1 | 52.424 | 3,815 |
| 4 | 14.754 | 13,556 |
| **8** | **11.233** | **17,804** |
| 16 | 11.468 | 17,439 |
| 32 | 11.502 | 17,389 |
| 64 | 11.680 | 17,123 |

8 并发达到本机的最佳点，此后没有继续提升。固定 8 并发后，1,000,000 行校准耗时 49.187 秒，既能稳定放大差异，又把单次正式测试控制在约 40～135 秒。

## 5. 正式结果

以下所有耗时均为重复测试的中位数。

### 5.1 事务边界与事务模式

协议固定为服务端 prepare，Async Commit/1PC 关闭，约束延迟检查。

| 行为 | 每事务语句/行数 | 重复 | 耗时（秒） | 行/秒 | 相对 autocommit | 波动 |
|---|---:|---:|---:|---:|---:|---:|
| autocommit | 1 / 100 | 3 | 49.167 | 20,339 | 基线 | 0.85% |
| explicit optimistic | 1 / 100 | 3 | 54.869 | 18,225 | +11.6% | 4.21% |
| explicit pessimistic | 1 / 100 | 3 | 53.783 | 18,593 | +9.4% | 0.32% |
| explicit optimistic | 10 / 1,000 | 5 | 44.899 | 22,272 | -8.7% | 8.37% |
| explicit pessimistic | 10 / 1,000 | 5 | 46.124 | 21,681 | -6.2% | 5.11% |
| **explicit optimistic** | **50 / 5,000** | **3** | **41.843** | **23,899** | **-14.9%** | **3.38%** |
| explicit pessimistic | 50 / 5,000 | 3 | 42.218 | 23,687 | -14.1% | 1.79% |

每事务 50 条语句把事务数从 autocommit 的 10,000 降到 200，摊薄了提交开销。代价是单个事务的 p95 延迟约 2 秒，失败重试也会扩大到 5,000 行；它优化的是总导入耗时，不是单事务延迟。

### 5.2 客户端协议

| 事务边界 | 协议 | 耗时（秒） | 行/秒 | 相对同组 prepare |
|---|---|---:|---:|---:|
| autocommit | 文本 `COM_QUERY` | 55.125 | 18,141 | +12.1% |
| autocommit | 客户端参数插值后 `COM_QUERY` | 57.744 | 17,318 | +17.4% |
| autocommit | **服务端 prepare / `COM_STMT_EXECUTE`** | **49.167** | **20,339** | 基线 |
| optimistic，50 条/事务 | 文本 `COM_QUERY` | 45.642 | 21,910 | +9.1% |
| optimistic，50 条/事务 | 客户端参数插值后 `COM_QUERY` | 48.265 | 20,719 | +15.3% |
| optimistic，50 条/事务 | **服务端 prepare / `COM_STMT_EXECUTE`** | **41.843** | **23,899** | 基线 |

客户端插值在 TiDB 侧仍是文本 SQL，既没有获得服务端 prepare 的复用收益，还在客户端生成更大的 SQL 字符串，因此本场景最慢。

### 5.3 TiDB 参数

基线为 optimistic、50 条/事务、服务端 prepare、打散键。

| 配置 | 耗时（秒） | 行/秒 | 相对对应基线 | 结论 |
|---|---:|---:|---:|---|
| Async Commit OFF，1PC OFF | 41.843 | 23,899 | 基线 | 普通提交基线 |
| Async Commit ON，1PC OFF | 42.037 | 23,788 | +0.5% | 无收益 |
| Async Commit OFF，1PC ON | 42.542 | 23,506 | +1.7% | 无收益 |
| Async Commit ON，1PC ON | 41.413 | 24,147 | -1.0% | 差异太小，不能归因 |
| prepared plan cache OFF | 42.075 | 23,767 | +0.6% | 建议保持 ON，但影响较小 |
| `tidb_constraint_check_in_place=ON` | 134.306 | 7,446 | +221.0% | 不适合本场景 |
| pessimistic 延迟约束检查 | 42.218 | 23,687 | 基线 | pessimistic 基线 |
| `tidb_constraint_check_in_place_pessimistic=ON` | 66.992 | 14,927 | +58.7% | 不适合本场景 |

TiDB 源码会把 `tidb_enable_async_commit` 和 `tidb_enable_1pc` 传入 KV 事务，但协议仍受事务 key 数、总 key 大小和 Region 分布等条件限制。本轮 Prometheus 中两项协议的 `ok`/`fallback` 计数始终为 0，而普通 commit 计数增加；因此不能把 ON/ON 的 1% 差异解释为协议优化。

### 5.4 其他对照

| 对照 | 耗时（秒） | 行/秒 | 差异 |
|---|---:|---:|---:|
| optimistic 50 条/事务，打散键，Async/1PC ON | 41.413 | 24,147 | 基线 |
| optimistic 50 条/事务，单调键，Async/1PC ON | 38.721 | 25,826 | -6.5% |
| autocommit 默认 optimistic，Async/1PC ON | 48.742 | 20,516 | 基线 |
| autocommit 强制 pessimistic，Async/1PC ON | 50.805 | 19,683 | +4.2% |

TiDB 在 autocommit 下默认选择 optimistic；只有开启 TiDB server 配置 `pessimistic-txn.pessimistic-auto-commit` 后，`tidb_txn_mode='pessimistic'` 才会使 autocommit 使用 pessimistic。该配置在这里没有收益，测试后已恢复为 `false`。

单调键结果只说明当前单机、8 并发、1,000,000 行窗口内的写入局部性收益更大。它没有覆盖多机、更高并发、长期 Region 热点或多写入端，不能据此推荐生产表统一采用单调 clustered 主键。

## 6. Prometheus 与 CPU profile

代表性 case 的 Prometheus 中位数：

| Case | TiDB 平均 CPU 核 | TiKV 平均 CPU 核 | 耗时（秒） |
|---|---:|---:|---:|
| autocommit prepare | 5.32 | 10.97 | 49.167 |
| optimistic 50 prepare | 5.88 | 13.13 | 41.843 |
| optimistic 50 prepare，Async/1PC ON | 5.92 | 11.97 | 41.413 |
| 上述配置，单调键 | 5.28 | 10.60 | 38.721 |
| optimistic，即时唯一约束检查 | 5.80 | 6.89 | 134.306 |
| pessimistic，即时唯一约束检查 | 3.86 | 8.29 | 66.992 |

Prometheus 采样间隔为 15 秒，因此 CPU 平均值适合观察量级和方向，不适合解释 1% 左右的小差异。

4 个代表 case 同时抓取 3 个 TiDB 实例各 30 秒 CPU profile，并合并分析：

- 正常 prepared 路径中，`runtime.mallocgc` 累计约占 31%～35%，`InsertValues.fastEvalRow` 约 18%～19%，`TableCommon.addRecord` 约 14%～20%，热点还包括 `CastValue`、`ExecBinaryParam` 和 insert 列表达式构建。主要成本是 50 列参数处理、类型转换、行/索引编码及分配。
- optimistic 即时唯一约束检查的 profile 中，`internal/runtime/syscall.Syscall6` 平坦占比 11.02%，`runtime.futex` 5.23%，`index.create` 累计 15.09%，`TableCommon.addRecord` 累计 27.57%。同时 TiKV CPU 降低、总耗时显著增加，表明新增的唯一键存储检查使执行更多地等待 KV/RPC。
- autocommit 与 50 条/事务的正常 profile 热点形态接近；显式大事务的主要收益不是消除行处理成本，而是减少提交次数。

## 7. 代码行为核对

基于与部署版本相同的 TiDB commit：

- `pkg/session/session.go:4217`：autocommit 事务默认使用 optimistic；只有 server 的 `pessimistic-auto-commit` 开启且 session mode 为 pessimistic 时才改用 pessimistic。
- `pkg/executor/insert.go:330`：optimistic 且 `tidb_constraint_check_in_place=OFF` 时使用 lazy duplicate-key check；pessimistic 也可推迟检查。
- `pkg/executor/insert.go:346`：显式 pessimistic 事务且 `tidb_constraint_check_in_place_pessimistic=OFF` 时，可把重复键检查推迟到 prewrite；autocommit 不能获得这项收益。
- `pkg/sessiontxn/isolation/base.go:479`：Async Commit 和 1PC session 开关会传入 KV 事务，但不保证事务满足协议条件。
- `docs/design/2024-01-09-pipelined-DML.md:44`：旧的 `tidb_enable_batch_dml` 路径已废弃且不安全，不应用来绕过事务大小限制。

## 8. 完整 case 清单

| Case | 中位耗时（秒） | 行/秒 | 重复 | 波动 |
|---|---:|---:|---:|---:|
| `auto_interpolate_default` | 57.744 | 17,318 | 3 | 1.09% |
| `auto_prepare_async_1pc` | 48.742 | 20,516 | 3 | 3.58% |
| `auto_prepare_async_1pc_pessimistic` | 50.805 | 19,683 | 3 | 1.74% |
| `auto_prepare_default` | 49.167 | 20,339 | 3 | 0.85% |
| `auto_text_default` | 55.125 | 18,141 | 3 | 0.40% |
| `optimistic_10_prepare_default` | 44.899 | 22,272 | 5 | 8.37% |
| `optimistic_1_prepare_default` | 54.869 | 18,225 | 3 | 4.21% |
| `optimistic_50_interpolate_default` | 48.265 | 20,719 | 3 | 3.32% |
| `optimistic_50_prepare_1pc_only` | 42.542 | 23,506 | 3 | 3.06% |
| `optimistic_50_prepare_async_1pc` | 41.413 | 24,147 | 3 | 2.14% |
| `optimistic_50_prepare_async_1pc_monotonic` | 38.721 | 25,826 | 3 | 2.94% |
| `optimistic_50_prepare_async_only` | 42.037 | 23,788 | 3 | 3.68% |
| `optimistic_50_prepare_constraint_in_place` | 134.306 | 7,446 | 3 | 0.84% |
| `optimistic_50_prepare_default` | 41.843 | 23,899 | 3 | 3.38% |
| `optimistic_50_prepare_plan_cache_off` | 42.075 | 23,767 | 3 | 0.57% |
| `optimistic_50_text_default` | 45.642 | 21,910 | 3 | 1.06% |
| `pessimistic_10_prepare_default` | 46.124 | 21,681 | 5 | 5.11% |
| `pessimistic_1_prepare_default` | 53.783 | 18,593 | 3 | 0.32% |
| `pessimistic_50_prepare_constraint_in_place` | 66.992 | 14,927 | 3 | 3.27% |
| `pessimistic_50_prepare_default` | 42.218 | 23,687 | 3 | 1.79% |

## 9. 结果边界

- 只测试了用户指定的每条 SQL 100 行，没有比较 10、500 或 1,000 行/语句。
- 数据无重复键且写入期间没有并发更新同一逻辑记录，不能代表冲突型 OLTP。
- 最多只比较到每事务 50 条语句，没有证明更大的事务会继续变快。
- 单机同部署使网络延迟、NUMA、磁盘与组件资源竞争不同于生产多机拓扑。
- 测试追求总导入耗时，不以单事务 p95/p99 或在线读请求受影响程度为目标。
- Async Commit/1PC 没有实际命中，本轮只能得出“对该宽表批次不是有效调优点”，不能评价它们在小事务中的收益。

## 10. 产物与复现

负载实现：

- `payload/insert_wide_benchmark.go`
- `scripts/insert-wide-benchmark/run-cases.sh`
- `scripts/insert-wide-benchmark/*.tsv`
- `scripts/insert-wide-benchmark/collect-prometheus.py`
- `scripts/insert-wide-benchmark/summarize-results.py`
- `scripts/insert-wide-benchmark/capture-profiles.sh`

代码分支：`codex/insert-wide-benchmark`。

远端原始产物保留在：

- `/root/cs/insert-test/results`：每次测试 JSON、日志、执行顺序和 manifest；
- `/root/cs/insert-test/prometheus`：每次测试的 Prometheus 原始序列与窗口摘要；
- `/root/cs/insert-test/profiles`：3 个 TiDB 的原始 profile 和合并 top；
- `/root/cs/insert-test/summary/results.csv` 与 `results.json`：20 个 case 的汇总；
- `/root/cs/insert-test/environment` 与 `calibration`：环境快照和校准结果。

实际接入方式与上线检查见[最佳实践](best-practices.md)。
