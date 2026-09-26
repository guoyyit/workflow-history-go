package history

// StepFunc 是回放时应用单条事件的纯函数。
// 给定当前状态与事件，返回新状态。确定性回放要求 StepFunc 无副作用、
// 仅依赖输入：同一历史与相同回放入口必须得到完全一致的结果。
type StepFunc func(state any, ev Event) (any, error)

// ReplaySession 表示一次从检查点出发的确定性回放会话。
//
// 会话持有游标（已应用的最大全局序号）与回放状态，二者构成续放点：
// 中断后调用 State() 取出游标与状态，之后用 ResumeReplay 即可从断点
// 继续推进，结果与不中断回放完全一致。
type ReplaySession struct {
	store      *Store
	instanceID string
	cursor     uint64 // 已应用的最大 Seq
	state      any    // 当前回放状态
	step       StepFunc
}

// StartReplay 从实例最近一次已提交检查点创建回放会话。
// initial 为检查点对应的初始状态（由调用方从检查点构造）。
// 会话将按全局序号顺序应用检查点之后的所有已提交事件。
func (s *Store) StartReplay(instanceID string, initial any, step StepFunc) (*ReplaySession, error) {
	cp, err := s.LatestCheckpoint(instanceID)
	if err != nil {
		return nil, err
	}
	return &ReplaySession{
		store:      s,
		instanceID: instanceID,
		cursor:     cp.Seq,
		state:      initial,
		step:       step,
	}, nil
}

// ResumeReplay 从中断位置继续回放：cursor 为已应用的最大 Seq，
// state 为该游标对应的回放状态（通常取自中断前 State() 的返回值）。
func (s *Store) ResumeReplay(instanceID string, cursor uint64, state any, step StepFunc) (*ReplaySession, error) {
	return &ReplaySession{
		store:      s,
		instanceID: instanceID,
		cursor:     cursor,
		state:      state,
		step:       step,
	}, nil
}

// Next 应用下一条已提交事件；返回 false 表示已追平当前已提交历史。
// 每次调用只推进一条事件，天然构成可中断的细粒度断点。
func (r *ReplaySession) Next() (bool, error) {
	evs, err := r.store.EventsSince(r.instanceID, r.cursor)
	if err != nil {
		return false, err
	}
	if len(evs) == 0 {
		return false, nil
	}
	ev := evs[0]
	ns, err := r.step(r.state, ev)
	if err != nil {
		return false, err
	}
	r.state = ns
	r.cursor = ev.Seq
	return true, nil
}

// State 返回当前回放游标与状态，可用于中断后通过 ResumeReplay 恢复。
func (r *ReplaySession) State() (cursor uint64, state any) {
	return r.cursor, r.state
}
