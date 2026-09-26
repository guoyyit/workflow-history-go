package history

import (
	"errors"
	"sync/atomic"
)

// ErrNoCheckpoint 表示实例尚无已提交检查点，禁止压缩。
var ErrNoCheckpoint = errors.New("history: no committed checkpoint, compaction forbidden")

// ErrHorizonUncovered 表示压缩 horizon 未被最近检查点覆盖。
var ErrHorizonUncovered = errors.New("history: horizon beyond latest checkpoint")

// RetentionPolicy 定义历史保留策略。
type RetentionPolicy struct {
	// MinKeepEvents 每个实例至少保留的最近事件条数，这些事件即使已被
	// 检查点覆盖也不会被压缩回收。
	MinKeepEvents int
}

// Compact 按保留策略压缩实例历史：将 horizon 之前、且已被最近一次
// 已提交检查点覆盖的事件合并为一条基线快照，并重写 WAL 回收空间。
//
// 一致性保证：
//   - horizon 不得超过最近检查点的逻辑时间，保证未被检查点覆盖的最近
//     事件绝不丢失；
//   - 无论 horizon 多大，至少保留 policy.MinKeepEvents 条最近事件；
//   - 压缩后，horizon 及之后的时间点查询结果逐字节不变；horizon 之前
//     的查询命中基线快照（Snapshot.Exact=false）；
//   - 重写采用临时文件 + 原子 rename，压缩过程中崩溃不会损坏既有历史。
func (s *Store) Compact(instanceID string, horizon uint64, policy RetentionPolicy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
	p, v := s.loadView(instanceID)
	if v.clock == 0 {
		return ErrNotFound
	}
	if v.lastCP == nil {
		return ErrNoCheckpoint
	}
	if horizon > v.lastCP.LogicalTime {
		return ErrHorizonUncovered
	}
	// 计算可丢弃事件：LogicalTime <= horizon，且不在最后 MinKeepEvents 条之内。
	keepFrom := len(v.events) - policy.MinKeepEvents
	if keepFrom < 0 {
		keepFrom = 0
	}
	dropEnd := 0 // [0, dropEnd) 为待丢弃区间
	for dropEnd < keepFrom && v.events[dropEnd].LogicalTime <= horizon {
		dropEnd++
	}
	if dropEnd == 0 {
		return nil // 没有可压缩的事件
	}
	last := v.events[dropEnd-1]
	base := &Checkpoint{
		InstanceID:  instanceID,
		LogicalTime: last.LogicalTime,
		Seq:         last.Seq,
		State:       last.To,
		Vars:        cloneVars(last.Vars),
	}
	kept := append([]Event(nil), v.events[dropEnd:]...)

	// 用全部实例的当前已提交视图重建 WAL（基线 + 保留事件 + 检查点），
	// 原子替换旧文件；目标实例使用压缩后的新基线与保留事件。
	var recs []record
	s.views.Range(func(key, val any) bool {
		vv := val.(*atomic.Pointer[instanceView]).Load()
		if vv == nil {
			return true
		}
		vbase, vevents := vv.base, vv.events
		if key.(string) == instanceID {
			vbase, vevents = base, kept
		}
		if vbase != nil {
			b := *vbase
			recs = append(recs, record{typ: recBase, base: &b})
		}
		for i := range vevents {
			ev := vevents[i]
			recs = append(recs, record{typ: recEvent, ev: &ev})
		}
		if vv.lastCP != nil {
			cp := *vv.lastCP
			recs = append(recs, record{typ: recCheckpoint, cp: &cp})
		}
		return true
	})
	if err := s.wal.rewrite(recs); err != nil {
		return err
	}
	// WAL 重写成功后更新内存视图。
	nv := &instanceView{events: kept, base: base, lastCP: v.lastCP, clock: v.clock}
	p.Store(nv)
	return nil
}
