# TiDB 宽表批量 INSERT 最佳实践

适用场景：TiDB `v7.1.9-0.1`，50 列宽表，2 个唯一索引、2 个普通索引，每条 `INSERT` 固定写 100 行，以完成大批量导入的总耗时为主要目标。

文档导航：[完整测试报告](test-report.md)

## 推荐方案

默认从下面的组合开始：

| 层面 | 推荐 |
|---|---|
| 客户端协议 | 服务端 prepare，prepare 一次后在同一连接反复执行 |
| 单条 SQL | 100 行，即本表 5,000 个参数 |
| 事务边界 | `BEGIN` → 50 条 `INSERT` → `COMMIT`，每事务 5,000 行 |
| 事务模式 | optimistic |
| 并发 | 本测试机 8 个长连接；生产环境重新做 4/8/16/32 梯度校准 |
| TiDB 端点 | 长连接均匀分配到可写 TiDB 实例 |
| 唯一约束检查 | `tidb_constraint_check_in_place=OFF` |
| prepare plan cache | 保持 ON |
| Async Commit / 1PC | 可以保持集群默认 ON，但不要把它们视为本场景的性能来源 |
| pessimistic autocommit | server 配置保持 `false` |

本测试中，推荐组合导入 1,000,000 行耗时 41.843 秒；服务端 prepare + autocommit 为 49.167 秒，推荐组合缩短 14.9%。

若希望保留当前集群默认的 Async Commit/1PC ON，实测为 41.413 秒，但其 1.0% 差异没有超出测试噪声，且监控确认事务未实际走这两种协议。

## 客户端写法

每个 worker 使用一个长连接，在连接建立后设置 session，并 prepare 一次固定形状的 100 行 SQL：

```sql
SET SESSION tidb_txn_mode = 'optimistic';
SET SESSION tidb_constraint_check_in_place = OFF;
SET SESSION tidb_enable_prepared_plan_cache = ON;
SET SESSION tidb_enable_async_commit = ON;
SET SESSION tidb_enable_1pc = ON;
```

写入循环：

```text
prepare INSERT ... VALUES (50 columns × 100 rows)

repeat:
    begin
    execute prepared INSERT 50 times
    commit
```

Go `database/sql` 的关键结构（贴近实际 API 的伪代码，错误分类与重试函数由业务实现）：

```go
db.SetMaxOpenConns(1)
db.SetMaxIdleConns(1)

stmt, err := db.PrepareContext(ctx, insert100RowsSQL)
if err != nil {
    return err
}
defer stmt.Close()

for hasMoreRows() {
    tx, err := db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    txStmt := tx.StmtContext(ctx, stmt)

    for i := 0; i < 50 && hasMoreRows(); i++ {
        if _, err = txStmt.ExecContext(ctx, next100RowsArgs()...); err != nil {
            txStmt.Close()
            _ = tx.Rollback()
            return retryOrFail(err)
        }
    }
    if err = txStmt.Close(); err != nil {
        _ = tx.Rollback()
        return err
    }
    if err = tx.Commit(); err != nil {
        return retryOrFail(err)
    }
}
```

注意：

- 不要每执行一条 SQL 就重新 prepare。
- 不要用客户端参数插值代替服务端 prepare。本测试中它比服务端 prepare 慢 13.3%～14.9%。
- 连接池中的 prepared statement 与物理连接相关。最容易控制的模型是每个写 worker 独占一个连接，或确认驱动能为池内物理连接正确管理 statement。
- commit 失败时按整个事务重试，而不是只补最后一条语句。数据生成必须确定且幂等，唯一键应能标识同一批数据。
- 应对可能重复提交的结果不确定错误时，先用业务唯一键确认提交结果，再决定是否重试。

## 为什么每事务 50 条

固定 100 行/语句时：

| 行为 | 每事务行数 | 1,000,000 行耗时 | 相对 autocommit |
|---|---:|---:|---:|
| autocommit | 100 | 49.167 秒 | 基线 |
| explicit optimistic，1 条/事务 | 100 | 54.869 秒 | 慢 11.6% |
| explicit optimistic，10 条/事务 | 1,000 | 44.899 秒 | 快 8.7% |
| **explicit optimistic，50 条/事务** | **5,000** | **41.843 秒** | **快 14.9%** |

显式事务包 1 条语句会增加 `BEGIN/COMMIT` 开销，不能优化提交。合并 50 条后，事务数从 10,000 降到 200，才获得明显收益。

50 条不是无条件越大越好。本轮没有测试 100 条或更大事务，而且 50 条/事务的 p95 已接近 2 秒。遇到以下情况应降到 10 条或更小：

- 单行实际 payload 明显大于本测试数据；
- 事务接近 `txn-total-size-limit` 或 session 内存限制；
- 锁冲突、写冲突或超时重试增加；
- 在线业务不能接受约 2 秒的事务延迟；
- 一次重试 5,000 行的成本过高；
- 导入明显影响在线读写延迟。

优先减小每事务语句数，不要直接提高事务大小或内存限制。不要使用已废弃且不安全的 `tidb_enable_batch_dml` 来拆分事务。

## optimistic 还是 pessimistic

无冲突、唯一键由客户端确定生成的纯导入优先 optimistic：

| 模式 | 1 条/事务 | 10 条/事务 | 50 条/事务 |
|---|---:|---:|---:|
| optimistic | 54.869 秒 | 44.899 秒 | **41.843 秒** |
| pessimistic | 53.783 秒 | 46.124 秒 | 42.218 秒 |

50 条/事务时两者只差 0.9%，因此模式不是主要性能杠杆。选择原则是：

- 新数据、键空间互不重叠、冲突很少：optimistic。
- 与在线业务并发写同一批 key，且 optimistic 重试成本已经通过监控证实较高：再测试 pessimistic。
- 不要仅因全局 `tidb_txn_mode=pessimistic` 就假设 autocommit 使用 pessimistic。TiDB autocommit 默认仍是 optimistic；server 的 `pessimistic-txn.pessimistic-auto-commit` 开启后才可能改变。本测试强制 pessimistic autocommit 慢 4.2%，建议保持该 server 配置为 `false`。

## 唯一约束参数

本场景最重要的 TiDB session 参数是：

```sql
SET SESSION tidb_constraint_check_in_place = OFF;
```

optimistic 50 条/事务时：

| 唯一约束检查 | 耗时 | 行/秒 |
|---|---:|---:|
| 延迟到提交阶段 | 41.843 秒 | 23,899 |
| 每次写入即时检查 | 134.306 秒 | 7,446 |

即时检查慢 221.0%。CPU profile 显示它引入了明显的 KV/RPC 等待和索引检查。

若确实使用 pessimistic，则同时保持：

```sql
SET SESSION tidb_constraint_check_in_place_pessimistic = OFF;
```

它允许显式 pessimistic 事务把重复键检查推迟到 prewrite。本测试中改为 ON 后耗时从 42.218 秒增至 66.992 秒，慢 58.7%。

延迟检查的行为边界：

- 重复键错误会更晚出现，通常在 commit/prewrite 阶段暴露。
- 任何重复键都可能使整个 5,000 行事务失败。
- 客户端必须把该事务视为一个原子重试单元。
- 如果业务必须尽早定位具体重复行，需在吞吐与错误定位之间取舍，并单独测试去重/预检查方案。
- `INSERT IGNORE`、`ON DUPLICATE KEY UPDATE` 不属于本测试覆盖的普通 INSERT 路径，不能直接套用结果。

## prepare 与 plan cache

| 事务边界 | 文本协议 | 客户端插值 | 服务端 prepare |
|---|---:|---:|---:|
| autocommit | 55.125 秒 | 57.744 秒 | **49.167 秒** |
| optimistic，50 条/事务 | 45.642 秒 | 48.265 秒 | **41.843 秒** |

服务端 prepare 是明确收益。保持 `tidb_enable_prepared_plan_cache=ON` 也是合理默认，但单独关闭它只使 41.843 秒增到 42.075 秒，影响约 0.6%。这说明主要收益来自协议与 statement 复用，不能把 prepare 的全部收益归因于 plan cache。

## Async Commit 与 1PC

本轮结果：

| Async Commit | 1PC | 耗时 |
|---|---|---:|
| OFF | OFF | 41.843 秒 |
| ON | OFF | 42.037 秒 |
| OFF | ON | 42.542 秒 |
| ON | ON | 41.413 秒 |

虽然 ON/ON 数字最低，但差距只有 1.0%。Prometheus 中 Async Commit 和 1PC 的成功计数均为 0，普通 commit 计数增加；宽表加 4 个二级索引导致的 mutation 数与 Region 分布使事务没有实际命中快速提交路径。

实践建议：

- 可以保持当前集群默认 ON，避免影响其他可能受益的小事务。
- 不要为这类 5,000 行宽表事务专门调整这两个开关，也不要宣称会带来 1% 收益。
- 上线后通过 `tidb_tikvclient_async_commit_txn_counter`、`tidb_tikvclient_one_pc_txn_counter` 和普通 commit 指标确认是否实际命中，而不只检查 session 变量值。

## 并发和键分布

本机 200,000 行校准中，1/4/8/16/32/64 并发的吞吐分别为 3,815、13,556、17,804、17,439、17,389、17,123 行/秒，因此选 8。

生产使用时：

- 用实际表、实际 payload、实际 TiDB/TiKV 拓扑重新扫 4/8/16/32 并发；
- 以总耗时下降是否停止、TiDB/TiKV CPU、写延迟和在线流量影响决定并发，不要照抄 8；
- 复用长连接，并把连接均匀分到 TiDB 节点；
- 达到平台后不要继续加连接，本测试中 16～64 已没有收益。

本机单调主键比打散主键快 6.5%，但这不是通用主键设计建议。多机、高并发、长期连续写入时，单调 clustered 主键可能形成 Region/Store 热点。表设计应以生产热点监控和长期压测为准；不要为了复现本轮单机最好数字而改变现有键分布。

## 上线检查清单

- 表结构、索引数量、平均行大小与测试接近。
- 每条 SQL 恰为 100 行，参数数未超过客户端/协议限制。
- 每个连接只 prepare 一次，并复用同一 SQL 形状。
- 默认 optimistic，每 50 条提交；先在预生产验证事务字节数和约 2 秒的 p95 是否可接受。
- `tidb_constraint_check_in_place=OFF`；pessimistic 时对应参数也为 OFF。
- `tidb_enable_prepared_plan_cache=ON`。
- `pessimistic-txn.pessimistic-auto-commit=false`。
- 客户端对整个事务做有界重试，数据生成幂等，并能处理提交结果不确定。
- 监控 TiDB/TiKV CPU、事务耗时、失败/重试、Region 热点、内存及事务大小。
- 若需要降低在线影响，先降并发或每事务语句数，不要牺牲正确性参数。

详细数据、环境和 profile 分析见[完整测试报告](test-report.md)。
