package history

import "sort"

// StateAt returns the committed variable snapshot at the given logical
// time: the state of the latest retained event with LogicalTime <= t.
// It never reflects writes committed after t. If t predates the oldest
// retained event, the oldest retained state is returned with Exact=false.
func (s *Service) StateAt(instanceID string, logicalTime int64) (Snapshot, error) {
	inst, ok := s.getInstance(instanceID)
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	v := inst.view.Load()
	if ev, ok := findEventAtTime(v, logicalTime); ok {
		return snapshotFromEvent(ev, true), nil
	}
	// t predates all retained events: fall back to the oldest retained
	// state (a compaction boundary when retention reclaimed the prefix).
	if ev, ok := firstEvent(v); ok {
		return snapshotFromEvent(ev, false), nil
	}
	if c := inst.checkpoint.Load(); c != nil {
		return Snapshot{
			InstanceID:  instanceID,
			Seq:         c.Seq,
			LogicalTime: c.LogicalTime,
			State:       c.State,
			Vars:        cloneVars(c.Vars),
			Exact:       false,
		}, nil
	}
	return Snapshot{}, ErrNotFound
}

// Current returns the latest committed snapshot of an instance.
func (s *Service) Current(instanceID string) (Snapshot, error) {
	inst, ok := s.getInstance(instanceID)
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	v := inst.view.Load()
	if v.lastSeq == 0 {
		return Snapshot{}, ErrNotFound
	}
	return Snapshot{
		InstanceID:  instanceID,
		Seq:         v.lastSeq,
		LogicalTime: v.lastTime,
		State:       v.state,
		Vars:        cloneVars(v.vars),
		Exact:       true,
	}, nil
}

// History returns retained events with from <= LogicalTime <= to, ordered
// by (LogicalTime, Seq).
func (s *Service) History(instanceID string, from, to int64) ([]Event, error) {
	inst, ok := s.getInstance(instanceID)
	if !ok {
		return nil, ErrNotFound
	}
	v := inst.view.Load()
	var out []Event
	collect := func(events []Event) {
		for _, ev := range events {
			if ev.LogicalTime < from || ev.LogicalTime > to {
				continue
			}
			out = append(out, cloneEvent(ev))
		}
	}
	for _, sg := range v.sealed {
		if sg.maxTime() < from || sg.minTime() > to {
			continue
		}
		collect(sg.events)
	}
	collect(v.active.events)
	return out, nil
}

func snapshotFromEvent(ev Event, exact bool) Snapshot {
	return Snapshot{
		InstanceID:  ev.InstanceID,
		Seq:         ev.Seq,
		LogicalTime: ev.LogicalTime,
		State:       ev.To,
		Vars:        cloneVars(ev.Vars),
		Exact:       exact,
	}
}

func cloneEvent(ev Event) Event {
	ev.Vars = cloneVars(ev.Vars)
	ev.Meta = cloneMeta(ev.Meta)
	return ev
}

// lastLETime returns the last event with LogicalTime <= t in a
// time-ordered slice.
func lastLETime(events []Event, t int64) (Event, bool) {
	i := sort.Search(len(events), func(i int) bool { return events[i].LogicalTime > t }) - 1
	if i < 0 {
		return Event{}, false
	}
	return events[i], true
}

// findEventAtTime returns the latest retained event with LogicalTime <= t.
func findEventAtTime(v *instanceView, t int64) (Event, bool) {
	if len(v.active.events) > 0 && v.active.minTime() <= t {
		if ev, ok := lastLETime(v.active.events, t); ok {
			return ev, true
		}
	}
	// Last sealed segment whose minTime <= t.
	idx := sort.Search(len(v.sealed), func(i int) bool { return v.sealed[i].minTime() > t }) - 1
	if idx >= 0 {
		return lastLETime(v.sealed[idx].events, t)
	}
	return Event{}, false
}

// findEventAtSeq returns the latest retained event with Seq <= seq.
func findEventAtSeq(v *instanceView, seq uint64) (Event, bool) {
	if len(v.active.events) > 0 && v.active.firstSeq() <= seq {
		if ev, ok := lastLESeq(v.active.events, seq); ok {
			return ev, true
		}
	}
	idx := sort.Search(len(v.sealed), func(i int) bool { return v.sealed[i].firstSeq() > seq }) - 1
	if idx >= 0 {
		return lastLESeq(v.sealed[idx].events, seq)
	}
	return Event{}, false
}

func lastLESeq(events []Event, seq uint64) (Event, bool) {
	i := sort.Search(len(events), func(i int) bool { return events[i].Seq > seq }) - 1
	if i < 0 {
		return Event{}, false
	}
	return events[i], true
}

// firstEvent returns the oldest retained event.
func firstEvent(v *instanceView) (Event, bool) {
	for _, sg := range v.sealed {
		if len(sg.events) > 0 {
			return sg.events[0], true
		}
	}
	if len(v.active.events) > 0 {
		return v.active.events[0], true
	}
	return Event{}, false
}

// eventsAfterSeq returns all retained events with Seq > floor in seq order.
func eventsAfterSeq(v *instanceView, floor uint64) []Event {
	var out []Event
	for _, sg := range v.sealed {
		for _, ev := range sg.events {
			if ev.Seq > floor {
				out = append(out, ev)
			}
		}
	}
	for _, ev := range v.active.events {
		if ev.Seq > floor {
			out = append(out, ev)
		}
	}
	return out
}
