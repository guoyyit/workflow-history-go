// Package history 提供本地工作流执行历史与回放服务。
//
// 每个工作流实例的状态迁移以不可变事件追加记录，事件携带全局单调序号、
// 实例内逻辑时间、变量快照与墙钟时间。服务支持按逻辑时间做时间点查询、
// 从检查点确定性回放、按保留策略压缩回收历史，并保证崩溃后可从最近一次
// 已提交检查点恢复且不丢失已确认历史。
package history

// Event 是一次已提交的状态迁移，提交后不可变。
type Event struct {
	InstanceID  string         // 工作流实例 ID
	Seq         uint64         // 全局单调递增序号，提交后唯一且不重复
	LogicalTime uint64         // 实例内逻辑时间（Lamport 时钟），从 1 开始递增
	From        string         // 迁移前状态
	To          string         // 迁移后状态
	Vars        map[string]any // 迁移完成后的完整变量快照
	WallTime    int64          // 提交时的墙钟时间（UnixNano）
}

// Transition 是提交一次状态迁移的输入。
type Transition struct {
	From string
	To   string
	Vars map[string]any // 迁移后的完整变量快照
}

// Checkpoint 是某个实例在某一逻辑时间点的已提交状态快照，
// 崩溃恢复与回放的起点。
type Checkpoint struct {
	InstanceID  string
	LogicalTime uint64         // 检查点覆盖到的逻辑时间
	Seq         uint64         // 检查点覆盖到的最大全局序号
	State       string         // 当前状态（最近一次迁移的 To）
	Vars        map[string]any // 该时间点的完整变量快照
}

// Snapshot 是时间点查询的结果。
type Snapshot struct {
	InstanceID  string
	LogicalTime uint64         // 实际命中的逻辑时间（<= 查询时间）
	State       string         // 该时间点的状态
	Vars        map[string]any // 该时间点的变量快照
	Exact       bool           // false 表示命中的是压缩基线，精度受保留策略限制
	Found       bool           // false 表示该实例在查询时间之前没有任何已提交事件
}
