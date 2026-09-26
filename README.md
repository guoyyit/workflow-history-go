# workflow-history-go

本地工作流执行历史与回放服务。为每个工作流实例记录状态迁移、变量快照
与时间信息，支持按逻辑时间查询任意时刻的历史状态，并允许从检查点确定
性回放后续执行。

## 核心概念

| 概念 | 说明 |
| --- | --- |
| `Event` | 一次已提交的状态迁移，不可变。携带全局序号 `Seq`、实例内逻辑时间 `LogicalTime`、迁移前后状态、迁移完成后的完整变量快照与墙钟时间。 |
| `Checkpoint` | 实例在某一逻辑时间点的已提交状态快照，是崩溃恢复与回放的起点。 |
| `Snapshot` | 时间点查询结果。`Exact=false` 表示命中压缩基线，精度受保留策略限制。 |
| `RetentionPolicy` | 历史保留策略，控制压缩回收行为。 |
| `ReplaySession` | 从检查点出发的确定性回放会话，持有游标，支持中断续放。 |

## 快速开始

```go
svc, _ := history.Open("./data")
defer svc.Close()
st := svc.Store()

// 1. 提交状态迁移（提交成功即已 fsync）
ev, _ := st.Commit("order-1", history.Transition{
    From: "created", To: "paid",
    Vars: map[string]any{"amount": 99.5},
})
fmt.Println(ev.Seq, ev.LogicalTime)

// 2. 时间点查询：只看逻辑时间 t 之前（含）已提交的状态
snap, _ := st.QueryAt("order-1", 1)

// 3. 建立检查点
cp, _ := st.SaveCheckpoint("order-1")

// 4. 从检查点确定性回放后续事件
rs, _ := st.StartReplay("order-1", myInitialState, myStepFunc)
for {
    ok, _ := rs.Next()
    if !ok { break }
}
cursor, state := rs.State() // 中断点：可稍后用 ResumeReplay 继续

// 5. 按保留策略压缩历史
st.Compact("order-1", cp.LogicalTime, history.RetentionPolicy{MinKeepEvents: 10})
```

## 一致性语义

### 提交与持久化

- 写路径串行化：分配全局序号 → 追加 WAL → `fsync` → 发布内存视图。
  `Commit` 返回成功即表示记录已持久化，崩溃后必然可见。
- 全局序号 `Seq` 从 1 开始单调递增、不重复；同一实例并发提交时被串行化，
  实例内逻辑时间 `LogicalTime` 从 1 开始连续递增。
- 崩溃恢复时截断 WAL 尾部校验失败或不完整的记录：**未完成写入永远不会
  作为已确认状态出现**，已确认历史完整可见。

### 读写并发

- 读路径完全无锁：每个实例的已提交视图是不可变对象，提交时整体替换
  （copy-on-write），读者通过原子指针加载视图。
- 读请求**永不阻塞写**，也总能观察到某一提交边界的完整一致快照，
  不会看到半提交状态。

### 时间点查询

- `QueryAt(id, t)` 返回逻辑时间 `t` 之前（含）最近一次已提交迁移后的
  完整状态，不混入更晚写入的内容。
- 变量快照随事件整体记录，查询结果即该时间点之后的第一次迁移前的完整
  变量集合，无需重放。

## 保留策略与压缩

`Compact(id, horizon, policy)` 将 `horizon` 之前的事件合并为一条基线快照，
并通过"临时文件 + 原子 rename"重写日志回收空间。安全约束：

1. `horizon` 不得超过最近一次已提交检查点的逻辑时间——**未被检查点覆盖
   的最近事件绝不丢失**；
2. 无论 `horizon` 多大，始终保留每个实例最近 `MinKeepEvents` 条事件；
3. 压缩后，`horizon` 及之后的时间点查询结果逐字节不变；`horizon` 之前
   的查询返回基线快照且 `Snapshot.Exact=false`——更早的细粒度历史已按
   保留策略回收，这是压缩的唯一可见影响；
4. 压缩只影响目标实例；压缩过程崩溃不会损坏既有历史。

## 崩溃恢复

- 重开目录即自动恢复：重放 WAL 重建全部实例视图、逻辑时钟与全局序号
  计数器（序号不复用）。
- `LatestCheckpoint` 返回最近一次已提交检查点，可作为恢复与回放入口。

## 确定性回放

- `StartReplay` 从最近检查点开始，按全局序号顺序逐条应用检查点之后的
  已提交事件；`StepFunc` 必须为无副作用的纯函数。
- 同一历史与相同回放入口必然得到完全一致的结果。
- `ReplaySession.State()` 返回游标与回放状态，构成续放点；中断后用
  `ResumeReplay(id, cursor, state, step)` 继续推进，结果与不中断回放一致。

## 限制

- 变量快照经 JSON 持久化：`Vars` 必须是可 JSON 序列化的值；崩溃恢复后
  数字统一为 `float64`（回放函数如需兼容请参考 `history/replay_test.go`
  中的写法）。
- 本服务为单进程本地嵌入式设计，WAL 文件不支持多进程同时写入。

## 测试

```sh
go test ./... -race
```

覆盖：并发写入全局有序、读写并发一致性、时间点查询、压缩回收及其安全
约束、崩溃恢复（撕裂尾部截断 / 未确认写入不可见）、确定性回放与中断
续放。
