package history

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// ErrClosed 表示存储已关闭。
var ErrClosed = errors.New("history: store closed")

// ErrNotFound 表示实例不存在或没有任何已提交历史。
var ErrNotFound = errors.New("history: instance not found")

// instanceView 是单个实例的已提交视图，构建后不可变；提交时整体替换
// （copy-on-write），读请求通过原子加载获得一致快照，不阻塞写入。
type instanceView struct {
	events []Event     // 按 LogicalTime 升序的已提交事件（均在 base 之后）
	base   *Checkpoint // 压缩基线；nil 表示未压缩
	lastCP *Checkpoint // 最近一次已提交检查点
	clock  uint64      // 实例内逻辑时钟（已提交的最大逻辑时间）
}

// stateAt 返回视图中逻辑时间 t 的状态。
func (v *instanceView) stateAt(instanceID string, t uint64) Snapshot {
	// events 按 LogicalTime 升序，找最后一个 <= t 的事件。
	lo, hi := 0, len(v.events)
	for lo < hi {
		mid := (lo + hi) / 2
		if v.events[mid].LogicalTime <= t {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo > 0 {
		ev := v.events[lo-1]
		return Snapshot{
			InstanceID:  instanceID,
			LogicalTime: ev.LogicalTime,
			State:       ev.To,
			Vars:        cloneVars(ev.Vars),
			Exact:       true,
			Found:       true,
		}
	}
	if v.base != nil {
		// t 落在基线覆盖范围内：t >= base.LogicalTime 时基线即真实状态；
		// t < base.LogicalTime 时更早历史已被压缩，只能返回基线。
		return Snapshot{
			InstanceID:  instanceID,
			LogicalTime: v.base.LogicalTime,
			State:       v.base.State,
			Vars:        cloneVars(v.base.Vars),
			Exact:       t >= v.base.LogicalTime,
			Found:       true,
		}
	}
	return Snapshot{InstanceID: instanceID}
}

// cloneVars 深拷贝变量快照，保证事件不可变、查询结果不被外部修改。
func cloneVars(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Store 是历史存储核心，保证多实例并发读写安全。
//
// 并发模型：写路径由 mu 序列化（分配序号 -> WAL 追加并 fsync -> 原子替换
// 视图）；读路径完全无锁，通过原子指针加载不可变视图，因此读请求永远不
// 阻塞写，且总能观察到某一提交边界的完整一致快照。
type Store struct {
	mu      sync.Mutex // 序列化写路径
	wal     *wal
	dir     string
	nextSeq uint64   // 下一个全局序号
	views   sync.Map // instanceID -> *atomic.Pointer[instanceView]
	closed  atomic.Bool
}

// walFileName 是存储目录下 WAL 文件名。
const walFileName = "wal.log"

// loadView 加载实例当前视图；不存在时返回空视图。
func (s *Store) loadView(instanceID string) (*atomic.Pointer[instanceView], *instanceView) {
	if p, ok := s.views.Load(instanceID); ok {
		ptr := p.(*atomic.Pointer[instanceView])
		if v := ptr.Load(); v != nil {
			return ptr, v
		}
		return ptr, &instanceView{}
	}
	ptr, _ := s.views.LoadOrStore(instanceID, new(atomic.Pointer[instanceView]))
	p := ptr.(*atomic.Pointer[instanceView])
	v := p.Load()
	if v == nil {
		v = &instanceView{}
	}
	return p, v
}

// openStore 打开目录下的存储并恢复已提交历史。
func openStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, nextSeq: 1}
	recs, err := readAll(filepath.Join(dir, walFileName))
	if err != nil {
		return nil, err
	}
	// 按记录顺序重建内存视图；WAL 中只存在已确认（fsync 成功）的记录，
	// 尾部未完成写入已被截断，不会作为已确认状态出现。
	for _, r := range recs {
		s.applyRecord(r)
	}
	w, err := openWAL(filepath.Join(dir, walFileName))
	if err != nil {
		return nil, err
	}
	s.wal = w
	return s, nil
}

// applyRecord 将一条 WAL 记录应用到内存视图（仅恢复与压缩重写时使用）。
func (s *Store) applyRecord(r record) {
	switch r.typ {
	case recEvent:
		ev := *r.ev
		p, v := s.loadView(ev.InstanceID)
		nv := &instanceView{
			events: append(append([]Event(nil), v.events...), ev),
			base:   v.base,
			lastCP: v.lastCP,
			clock:  maxUint64(v.clock, ev.LogicalTime),
		}
		p.Store(nv)
		if ev.Seq >= s.nextSeq {
			s.nextSeq = ev.Seq + 1
		}
	case recCheckpoint:
		cp := *r.cp
		p, v := s.loadView(cp.InstanceID)
		nv := &instanceView{events: v.events, base: v.base, lastCP: &cp, clock: maxUint64(v.clock, cp.LogicalTime)}
		p.Store(nv)
	case recBase:
		base := *r.base
		p, v := s.loadView(base.InstanceID)
		// 丢弃被基线覆盖的事件（防御性；压缩重写后本不应存在）。
		kept := v.events[:0:0]
		for _, ev := range v.events {
			if ev.LogicalTime > base.LogicalTime {
				kept = append(kept, ev)
			}
		}
		nv := &instanceView{events: kept, base: &base, lastCP: v.lastCP, clock: maxUint64(v.clock, base.LogicalTime)}
		p.Store(nv)
	}
}

// Commit 追加一次状态迁移，返回已提交事件。
// 提交成功即已 fsync，崩溃后仍可见；同一实例并发提交由 mu 序列化，
// 全局序号唯一且按提交顺序递增。
func (s *Store) Commit(instanceID string, tr Transition) (Event, error) {
	if instanceID == "" {
		return Event{}, errors.New("history: empty instance id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return Event{}, ErrClosed
	}
	p, v := s.loadView(instanceID)
	ev := Event{
		InstanceID:  instanceID,
		Seq:         s.nextSeq,
		LogicalTime: v.clock + 1,
		From:        tr.From,
		To:          tr.To,
		Vars:        cloneVars(tr.Vars),
		WallTime:    time.Now().UnixNano(),
	}
	// 先持久化并 fsync，再发布到内存视图：保证已确认历史崩溃后不丢，
	// 且未完成写入永远不会出现在已确认视图中。
	if err := s.wal.append(record{typ: recEvent, ev: &ev}); err != nil {
		return Event{}, err
	}
	nv := &instanceView{
		events: append(append([]Event(nil), v.events...), ev),
		base:   v.base,
		lastCP: v.lastCP,
		clock:  ev.LogicalTime,
	}
	p.Store(nv)
	s.nextSeq++
	return ev, nil
}

// SaveCheckpoint 为实例当前已提交状态提交一个检查点。
func (s *Store) SaveCheckpoint(instanceID string) (Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return Checkpoint{}, ErrClosed
	}
	p, v := s.loadView(instanceID)
	if v.clock == 0 {
		return Checkpoint{}, ErrNotFound
	}
	cp := Checkpoint{
		InstanceID:  instanceID,
		LogicalTime: v.clock,
		State:       v.stateAt(instanceID, v.clock).State,
		Vars:        v.stateAt(instanceID, v.clock).Vars,
	}
	if n := len(v.events); n > 0 {
		cp.Seq = v.events[n-1].Seq
	} else if v.base != nil {
		cp.Seq = v.base.Seq
	}
	if err := s.wal.append(record{typ: recCheckpoint, cp: &cp}); err != nil {
		return Checkpoint{}, err
	}
	nv := &instanceView{events: v.events, base: v.base, lastCP: &cp, clock: v.clock}
	p.Store(nv)
	return cp, nil
}

// LatestCheckpoint 返回实例最近一次已提交检查点。
func (s *Store) LatestCheckpoint(instanceID string) (Checkpoint, error) {
	_, v := s.loadView(instanceID)
	if v.lastCP == nil {
		return Checkpoint{}, ErrNotFound
	}
	cp := *v.lastCP
	cp.Vars = cloneVars(cp.Vars)
	return cp, nil
}

// QueryAt 查询实例在逻辑时间 t 的已提交状态。
// 结果只反映 t 之前（含）已提交的完整状态，不混入更晚写入的内容；
// 无锁读取，与并发写互不阻塞。
func (s *Store) QueryAt(instanceID string, t uint64) (Snapshot, error) {
	if s.closed.Load() {
		return Snapshot{}, ErrClosed
	}
	_, v := s.loadView(instanceID)
	snap := v.stateAt(instanceID, t)
	if !snap.Found {
		return snap, ErrNotFound
	}
	return snap, nil
}

// EventsSince 返回实例中 Seq > afterSeq 的已提交事件（按 Seq 升序）。
func (s *Store) EventsSince(instanceID string, afterSeq uint64) ([]Event, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	_, v := s.loadView(instanceID)
	var out []Event
	for _, ev := range v.events {
		if ev.Seq > afterSeq {
			out = append(out, ev)
		}
	}
	return out, nil
}

// Close 关闭存储。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Swap(true) {
		return nil
	}
	return s.wal.close()
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
