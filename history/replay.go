package history

// ReplayOptions selects the replay entry point.
type ReplayOptions struct {
	// CheckpointSeq selects a specific checkpoint; 0 means latest.
	CheckpointSeq uint64
	// AfterSeq is a resume cursor: events with Seq <= max(checkpoint.Seq,
	// AfterSeq) are skipped.
	AfterSeq uint64
}

// ReplayStep is one deterministic replay step.
type ReplayStep struct {
	Event Event
	State Snapshot // reconstructed state after applying Event
}

// ReplaySession iterates retained events from a checkpoint forward, in
// global sequence order. It is deterministic: the same retained history
// and the same entry point always yield the same steps. A session can be
// abandoned at any point and resumed later via ReplayOptions.AfterSeq =
// session.Cursor(). The session iterates a stable snapshot of the
// retained history taken at start, so concurrent commits do not disturb
// an in-flight replay.
type ReplaySession struct {
	steps  []Event
	base   Snapshot
	idx    int
	cursor uint64
}

// StartReplay opens a replay session for an instance. The replay floor is
// max(checkpoint.Seq, opts.AfterSeq): the session starts at the state of
// the floor and delivers every retained event after it, in seq order.
func (s *Service) StartReplay(instanceID string, opts ReplayOptions) (*ReplaySession, error) {
	inst, ok := s.getInstance(instanceID)
	if !ok {
		return nil, ErrNotFound
	}
	var ckpt Checkpoint
	if opts.CheckpointSeq != 0 {
		c, ok, err := loadCheckpointBySeq(s.dir, instanceID, opts.CheckpointSeq)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrNoCheckpoint
		}
		ckpt = c
	} else {
		c := inst.checkpoint.Load()
		if c == nil {
			return nil, ErrNoCheckpoint
		}
		ckpt = *c
	}
	floor := ckpt.Seq
	if opts.AfterSeq > floor {
		floor = opts.AfterSeq
	}
	v := inst.view.Load()
	var base Snapshot
	if floor == ckpt.Seq {
		base = Snapshot{
			InstanceID:  instanceID,
			Seq:         ckpt.Seq,
			LogicalTime: ckpt.LogicalTime,
			State:       ckpt.State,
			Vars:        cloneVars(ckpt.Vars),
			Exact:       true,
		}
	} else {
		ev, ok := findEventAtSeq(v, floor)
		if !ok {
			// The resume cursor itself was reclaimed; cannot continue
			// deterministically from it.
			return nil, ErrHistoryReclaimed
		}
		base = snapshotFromEvent(ev, true)
	}
	return &ReplaySession{
		steps:  eventsAfterSeq(v, floor),
		base:   base,
		cursor: floor,
	}, nil
}

// Next returns the next step, or false when the replay is exhausted.
func (r *ReplaySession) Next() (ReplayStep, bool) {
	if r.idx >= len(r.steps) {
		return ReplayStep{}, false
	}
	ev := r.steps[r.idx]
	r.idx++
	r.base = snapshotFromEvent(ev, true)
	r.cursor = ev.Seq
	return ReplayStep{Event: cloneEvent(ev), State: r.base}, true
}

// Cursor returns the sequence number of the last delivered event; it can
// be used to resume an interrupted replay.
func (r *ReplaySession) Cursor() uint64 { return r.cursor }
