package history

// Compact merges and exactly compacts eligible sealed segments of an
// instance: for each logical time only the latest event is kept, which
// never changes the result of point-in-time queries. Only segments fully
// covered by the latest checkpoint and older than the newest
// KeepSegments segments are eligible.
func (s *Service) Compact(instanceID string) (CompactReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return CompactReport{}, ErrClosed
	}
	return s.compactLocked(instanceID)
}

// Reclaim drops sealed segments whose max logical time is <= beforeTime,
// replacing them with a single summary event carrying the state at the
// end of the reclaimed range. Only checkpoint-covered segments beyond the
// newest KeepSegments segments are eligible, so recent events not covered
// by any checkpoint are never lost.
func (s *Service) Reclaim(instanceID string, beforeTime int64) (CompactReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return CompactReport{}, ErrClosed
	}
	return s.reclaimLocked(instanceID, beforeTime)
}

// Maintain applies the configured retention policy once.
func (s *Service) Maintain(instanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.maintainLocked(instanceID)
}

// maintainLocked applies the retention policy; caller must hold s.mu.
// Reclaim runs before compact: a freshly merged prefix segment would
// span recent times and block horizon-based reclamation.
func (s *Service) maintainLocked(instanceID string) error {
	if s.policy.ReclaimHorizon > 0 {
		inst, ok := s.getInstance(instanceID)
		if !ok {
			return ErrNotFound
		}
		v := inst.view.Load()
		if _, err := s.reclaimLocked(instanceID, v.lastTime-s.policy.ReclaimHorizon); err != nil {
			return err
		}
	}
	_, err := s.compactLocked(instanceID)
	return err
}

// eligibleSegments returns the leading run of sealed segments that are
// covered by the latest checkpoint, excluding the newest KeepSegments.
func (s *Service) eligibleSegments(inst *instanceState, v *instanceView) []*segment {
	ckpt := inst.checkpoint.Load()
	if ckpt == nil {
		return nil
	}
	sealed := v.sealed
	if keep := s.policy.KeepSegments; keep > 0 && len(sealed) > keep {
		sealed = sealed[:len(sealed)-keep]
	} else if keep > 0 {
		sealed = nil
	}
	n := 0
	for n < len(sealed) && sealed[n].lastSeq() <= ckpt.Seq {
		n++
	}
	return sealed[:n]
}

func (s *Service) compactLocked(instanceID string) (CompactReport, error) {
	inst, ok := s.getInstance(instanceID)
	if !ok {
		return CompactReport{}, ErrNotFound
	}
	if inst.checkpoint.Load() == nil {
		return CompactReport{}, ErrNoCheckpoint
	}
	v := inst.view.Load()
	eligible := s.eligibleSegments(inst, v)
	rep := CompactReport{InstanceID: instanceID, SegmentsBefore: len(v.sealed), SegmentsAfter: len(v.sealed)}
	for _, sg := range eligible {
		rep.EventsBefore += len(sg.events)
	}
	rep.EventsAfter = rep.EventsBefore
	if len(eligible) == 0 {
		return rep, nil
	}
	// Merge eligible segments, keeping only the latest event per logical
	// time. This is exact: StateAt(t) always wants the latest event <= t.
	var merged []Event
	for _, sg := range eligible {
		for _, ev := range sg.events {
			if n := len(merged); n > 0 && merged[n-1].LogicalTime == ev.LogicalTime {
				merged[n-1] = ev // same logical time: keep the later event
			} else {
				merged = append(merged, ev)
			}
		}
	}
	if len(eligible) == 1 && len(merged) == len(eligible[0].events) {
		return rep, nil // nothing to save
	}
	if err := s.replaceSegmentsLocked(inst, v, eligible, merged); err != nil {
		return CompactReport{}, err
	}
	rep.SegmentsAfter = len(v.sealed) - len(eligible) + 1
	rep.EventsAfter = len(merged)
	return rep, nil
}

func (s *Service) reclaimLocked(instanceID string, beforeTime int64) (CompactReport, error) {
	inst, ok := s.getInstance(instanceID)
	if !ok {
		return CompactReport{}, ErrNotFound
	}
	if inst.checkpoint.Load() == nil {
		return CompactReport{}, ErrNoCheckpoint
	}
	v := inst.view.Load()
	eligible := s.eligibleSegments(inst, v)
	rep := CompactReport{InstanceID: instanceID, SegmentsBefore: len(v.sealed), SegmentsAfter: len(v.sealed)}
	n := 0
	for n < len(eligible) && eligible[n].maxTime() <= beforeTime {
		n++
	}
	eligible = eligible[:n]
	if len(eligible) == 0 {
		return rep, nil
	}
	for _, sg := range eligible {
		rep.EventsBefore += len(sg.events)
	}
	// Summarize the reclaimed range by its final state.
	last := eligible[len(eligible)-1].events
	tail := last[len(last)-1]
	summary := Event{
		Seq:         tail.Seq,
		InstanceID:  tail.InstanceID,
		LogicalTime: tail.LogicalTime,
		Kind:        KindSummary,
		To:          tail.To,
		Vars:        cloneVars(tail.Vars),
		WallTime:    tail.WallTime,
	}
	if err := s.replaceSegmentsLocked(inst, v, eligible, []Event{summary}); err != nil {
		return CompactReport{}, err
	}
	rep.SegmentsAfter = len(v.sealed) - len(eligible) + 1
	rep.EventsAfter = 1
	return rep, nil
}

// replaceSegmentsLocked swaps oldSegs for a new segment built from
// newEvents: write the new segment file, publish a new manifest, delete
// the old files, and publish a new view. Crash safety: the manifest is
// only updated after the new file is durable; orphaned files are cleaned
// on recovery.
func (s *Service) replaceSegmentsLocked(inst *instanceState, v *instanceView, oldSegs []*segment, newEvents []Event) error {
	name, err := writeSegmentFile(s.dir, inst.id, newEvents, s.opts.NoFsync)
	if err != nil {
		return err
	}
	oldSet := map[*segment]bool{}
	for _, sg := range oldSegs {
		oldSet[sg] = true
	}
	newSeg := &segment{events: newEvents}
	var sealed []*segment
	inserted := false
	for _, sg := range v.sealed {
		if oldSet[sg] {
			if !inserted {
				sealed = append(sealed, newSeg)
				inserted = true
			}
			continue
		}
		sealed = append(sealed, sg)
	}
	if !inserted {
		sealed = append(sealed, newSeg)
	}
	// Publish the manifest referencing exactly the live files, in the
	// same seq order as the new sealed list. The old manifest and
	// v.sealed are both seq-ordered, so they align by index.
	oldM, _, err := loadSegManifest(s.dir, inst.id)
	if err != nil {
		return err
	}
	fileOf := map[*segment]string{}
	for i, sg := range v.sealed {
		if i < len(oldM.Files) {
			fileOf[sg] = oldM.Files[i]
		}
	}
	m := segManifest{Instance: inst.id}
	for _, sg := range sealed {
		if sg == newSeg {
			m.Files = append(m.Files, name)
		} else {
			m.Files = append(m.Files, fileOf[sg])
		}
	}
	if err := writeSegManifest(s.dir, m, s.opts.NoFsync); err != nil {
		return err
	}
	// Delete replaced files (best effort; orphans are cleaned on recovery).
	live := map[string]bool{}
	for _, n := range m.Files {
		live[n] = true
	}
	all, err := listSegmentFiles(s.dir, inst.id)
	if err != nil {
		return err
	}
	for _, n := range all {
		if !live[n] {
			_ = removeFile(segDir(s.dir, inst.id), n)
		}
	}
	// Publish the new view.
	nv := &instanceView{
		sealed:   sealed,
		active:   v.active,
		lastSeq:  v.lastSeq,
		lastTime: v.lastTime,
		state:    v.state,
		vars:     v.vars,
	}
	inst.view.Store(nv)
	return nil
}
