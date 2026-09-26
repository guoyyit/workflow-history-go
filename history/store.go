package history

import (
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Service is the workflow history & replay service.
//
// Concurrency model: writers serialize on Service.mu, which establishes
// the global event order and hands out unique sequence numbers. Readers
// load an immutable per-instance view via an atomic pointer and never
// block writers. An event becomes visible only after its WAL record is
// durable, so "confirmed" implies "recoverable".
type Service struct {
	dir    string
	opts   Options
	policy RetentionPolicy

	mu        sync.Mutex // serializes writers; establishes the global event order
	seq       uint64     // last assigned global sequence number
	wal       *wal
	instances sync.Map // map[string]*instanceState
	closed    bool
}

// instanceState holds the mutable (under Service.mu) bookkeeping and the
// atomically published immutable read view of one instance.
type instanceState struct {
	id             string
	view           atomic.Pointer[instanceView]
	checkpoint     atomic.Pointer[Checkpoint]
	sealedCoverage uint64 // highest seq sealed into segment files (under Service.mu)
}

// instanceView is an immutable snapshot of an instance's retained history.
// Readers load it atomically and never block writers.
type instanceView struct {
	sealed   []*segment // immutable, ordered by seq
	active   *segment   // copy-on-write
	lastSeq  uint64
	lastTime int64
	state    string
	vars     map[string][]byte
}

func (o Options) defaults() Options {
	if o.SegmentSize <= 0 {
		o.SegmentSize = 64
	}
	if o.KeepCheckpoints <= 0 {
		o.KeepCheckpoints = 3
	}
	return o
}

// Open opens (or creates) a service rooted at dir and recovers committed
// state from checkpoints, segment files and the WAL.
func Open(dir string, opts Options) (*Service, error) {
	opts = opts.defaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	cleanTmpFiles(dir)
	w, batches, err := openWAL(dir, opts.NoFsync)
	if err != nil {
		return nil, err
	}
	s := &Service{dir: dir, opts: opts, wal: w}
	if err := s.recover(batches); err != nil {
		w.close()
		return nil, err
	}
	return s, nil
}

// Close flushes and closes the service.
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.wal.close()
}

// Commit appends one transition as an immutable event. The event is
// durable (fsynced) before it becomes visible to readers.
func (s *Service) Commit(instanceID string, tr Transition) (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Event{}, ErrClosed
	}
	inst := s.getOrCreateInstance(instanceID)
	v := inst.view.Load()
	if tr.LogicalTime < v.lastTime {
		return Event{}, ErrNonMonotonicTime
	}
	ev := Event{
		Seq:         s.seq + 1,
		InstanceID:  instanceID,
		LogicalTime: tr.LogicalTime,
		Kind:        KindTransition,
		From:        tr.From,
		To:          tr.To,
		Vars:        cloneVars(tr.Vars),
		Meta:        cloneMeta(tr.Meta),
		WallTime:    time.Now().UTC(),
	}
	if err := s.wal.appendBatch([]Event{ev}); err != nil {
		return Event{}, err
	}
	s.seq = ev.Seq
	nv := &instanceView{
		sealed:   v.sealed,
		active:   &segment{events: append(append([]Event(nil), v.active.events...), ev)},
		lastSeq:  ev.Seq,
		lastTime: ev.LogicalTime,
		state:    ev.To,
		vars:     ev.Vars,
	}
	if len(nv.active.events) >= s.opts.SegmentSize {
		if err := s.sealLocked(inst, nv); err != nil {
			return Event{}, err
		}
	}
	inst.view.Store(nv)
	return ev, nil
}

// SetRetentionPolicy installs the retention policy used by Maintain.
func (s *Service) SetRetentionPolicy(p RetentionPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.policy = p
}

func (s *Service) getInstance(id string) (*instanceState, bool) {
	v, ok := s.instances.Load(id)
	if !ok {
		return nil, false
	}
	return v.(*instanceState), true
}

func (s *Service) getOrCreateInstance(id string) *instanceState {
	if inst, ok := s.getInstance(id); ok {
		return inst
	}
	inst := &instanceState{id: id}
	inst.view.Store(&instanceView{active: &segment{}})
	actual, _ := s.instances.LoadOrStore(id, inst)
	return actual.(*instanceState)
}

// sealLocked seals nv.active of inst into a segment file, publishes the
// manifest and rewrites the WAL without the sealed events. On return nv
// has the sealed segment appended and an empty active segment.
func (s *Service) sealLocked(inst *instanceState, nv *instanceView) error {
	if len(nv.active.events) == 0 {
		return nil
	}
	name, err := writeSegmentFile(s.dir, inst.id, nv.active.events, s.opts.NoFsync)
	if err != nil {
		return err
	}
	sealedSeg := &segment{events: nv.active.events}
	nv.sealed = append(append([]*segment(nil), nv.sealed...), sealedSeg)
	nv.active = &segment{}
	inst.sealedCoverage = sealedSeg.lastSeq()

	m, _, err := loadSegManifest(s.dir, inst.id)
	if err != nil {
		return err
	}
	m.Instance = inst.id
	m.Files = append(m.Files, name)
	if err := writeSegManifest(s.dir, m, s.opts.NoFsync); err != nil {
		return err
	}
	return s.rewriteWALLocked(inst, nv)
}

// rewriteWALLocked persists only the still-unsealed events of all
// instances, keeping the WAL small.
func (s *Service) rewriteWALLocked(except *instanceState, exceptView *instanceView) error {
	var batches [][]Event
	s.instances.Range(func(_, val any) bool {
		ist := val.(*instanceState)
		v := exceptView
		if ist != except {
			v = ist.view.Load()
		}
		if v != nil && len(v.active.events) > 0 {
			batches = append(batches, v.active.events)
		}
		return true
	})
	return s.wal.rewrite(batches)
}

// recover rebuilds in-memory state after a restart.
func (s *Service) recover(batches [][]Event) error {
	names := map[string]bool{}
	for _, sub := range []string{"checkpoints", "segments"} {
		entries, err := os.ReadDir(filepath.Join(s.dir, sub))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			if id, err := url.PathUnescape(e.Name()); err == nil {
				names[id] = true
			}
		}
	}

	maxSeq := uint64(0)
	sealedByInst := map[string][]*segment{}
	for id := range names {
		inst := s.getOrCreateInstance(id)
		if ck, ok, err := loadLatestCheckpoint(s.dir, id); err != nil {
			return err
		} else if ok {
			ck := ck
			inst.checkpoint.Store(&ck)
			if ck.Seq > maxSeq {
				maxSeq = ck.Seq
			}
		}
		sealed, coverage, err := s.loadSegments(id)
		if err != nil {
			return err
		}
		inst.sealedCoverage = coverage
		sealedByInst[id] = sealed
		for _, sg := range sealed {
			if sg.lastSeq() > maxSeq {
				maxSeq = sg.lastSeq()
			}
		}
	}

	activeByInst := map[string][]Event{}
	for _, b := range batches {
		for _, ev := range b {
			inst := s.getOrCreateInstance(ev.InstanceID)
			names[ev.InstanceID] = true
			if ev.Seq <= inst.sealedCoverage {
				continue // already sealed into a segment file
			}
			activeByInst[ev.InstanceID] = append(activeByInst[ev.InstanceID], ev)
			if ev.Seq > maxSeq {
				maxSeq = ev.Seq
			}
		}
	}

	for id := range names {
		inst := s.getOrCreateInstance(id)
		act := activeByInst[id]
		sort.Slice(act, func(i, j int) bool { return act[i].Seq < act[j].Seq })
		inst.view.Store(buildView(inst.checkpoint.Load(), sealedByInst[id], &segment{events: act}))
	}
	s.seq = maxSeq
	return nil
}

// loadSegments reads the manifest and all referenced segment files, and
// removes orphan files not referenced by the manifest. The returned
// segments and the persisted manifest are kept in the same seq order so
// later compactions can align them by index.
func (s *Service) loadSegments(id string) ([]*segment, uint64, error) {
	m, ok, err := loadSegManifest(s.dir, id)
	if err != nil {
		return nil, 0, err
	}
	type pair struct {
		name string
		seg  *segment
	}
	var pairs []pair
	live := map[string]bool{}
	if ok {
		for _, name := range m.Files {
			events, err := readSegmentFile(filepath.Join(segDir(s.dir, id), name))
			if err != nil {
				return nil, 0, err
			}
			pairs = append(pairs, pair{name, &segment{events: events}})
			live[name] = true
		}
	}
	// Orphan cleanup: files not referenced by the manifest were written by
	// an interrupted seal/compaction; their events are still in the WAL or
	// superseded by a newer manifest.
	all, err := listSegmentFiles(s.dir, id)
	if err != nil {
		return nil, 0, err
	}
	for _, name := range all {
		if !live[name] {
			os.Remove(filepath.Join(segDir(s.dir, id), name))
		}
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].seg.firstSeq() < pairs[j].seg.firstSeq() })
	var sealed []*segment
	var names []string
	sorted := true
	for i, p := range pairs {
		sealed = append(sealed, p.seg)
		names = append(names, p.name)
		if i < len(m.Files) && m.Files[i] != p.name {
			sorted = false
		}
	}
	if ok && !sorted {
		// Persist the canonical seq order.
		if err := writeSegManifest(s.dir, segManifest{Instance: id, Files: names}, s.opts.NoFsync); err != nil {
			return nil, 0, err
		}
	}
	var coverage uint64
	if len(sealed) > 0 {
		coverage = sealed[len(sealed)-1].lastSeq()
	}
	return sealed, coverage, nil
}

// buildView computes the current committed state: the checkpoint base
// overlaid with all retained events newer than the checkpoint.
func buildView(ckpt *Checkpoint, sealed []*segment, active *segment) *instanceView {
	v := &instanceView{sealed: sealed, active: active}
	var baseSeq uint64
	if ckpt != nil {
		v.lastSeq = ckpt.Seq
		v.lastTime = ckpt.LogicalTime
		v.state = ckpt.State
		v.vars = cloneVars(ckpt.Vars)
		baseSeq = ckpt.Seq
	}
	apply := func(ev Event) {
		if ev.Seq <= baseSeq {
			return
		}
		v.lastSeq = ev.Seq
		v.lastTime = ev.LogicalTime
		v.state = ev.To
		v.vars = ev.Vars // events are immutable; safe to share
	}
	for _, sg := range sealed {
		for _, ev := range sg.events {
			apply(ev)
		}
	}
	for _, ev := range active.events {
		apply(ev)
	}
	return v
}

// cleanTmpFiles removes leftover temporary files from interrupted atomic
// writes, best effort.
func cleanTmpFiles(dir string) {
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && filepath.Ext(path) == ".tmp" {
			os.Remove(path)
		}
		return nil
	})
}

func cloneVars(in map[string][]byte) map[string][]byte {
	if in == nil {
		return nil
	}
	out := make(map[string][]byte, len(in))
	for k, v := range in {
		out[k] = append([]byte(nil), v...)
	}
	return out
}

func cloneMeta(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
