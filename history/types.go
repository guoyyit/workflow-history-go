// Package history provides a local workflow execution history and replay
// service. It records state transitions of workflow instances as immutable
// events with variable snapshots and logical timestamps, supports
// point-in-time queries, checkpointing, retention-driven compaction and
// deterministic replay.
package history

import (
	"errors"
	"time"
)

// EventKind classifies stored events.
type EventKind string

const (
	// KindTransition is a regular state transition event.
	KindTransition EventKind = "transition"
	// KindSummary is a synthetic event produced by retention reclamation.
	// It carries the full variable snapshot at the end of a reclaimed
	// range so point-in-time queries and replay stay well defined.
	KindSummary EventKind = "summary"
)

// Event is an immutable record of one state transition.
type Event struct {
	Seq         uint64            `json:"seq"`         // global, monotonic, unique
	InstanceID  string            `json:"instance"`    // owning workflow instance
	LogicalTime int64             `json:"logicalTime"` // instance-local logical time
	Kind        EventKind         `json:"kind"`        // transition or summary
	From        string            `json:"from,omitempty"`
	To          string            `json:"to,omitempty"`
	Vars        map[string][]byte `json:"vars,omitempty"` // variable snapshot after the transition
	Meta        map[string]string `json:"meta,omitempty"`
	WallTime    time.Time         `json:"wallTime"`
}

// Transition is the client-supplied description of a state transition.
type Transition struct {
	LogicalTime int64
	From        string
	To          string
	Vars        map[string][]byte
	Meta        map[string]string
}

// Snapshot is the result of a point-in-time query.
type Snapshot struct {
	InstanceID  string
	Seq         uint64
	LogicalTime int64
	State       string
	Vars        map[string][]byte
	// Exact is false when the requested time predates the oldest retained
	// event; the snapshot then reflects the oldest retained state (a
	// compaction boundary) rather than the exact state at that time.
	Exact bool
}

// Checkpoint is a durable, atomically published snapshot of an instance.
type Checkpoint struct {
	InstanceID  string            `json:"instance"`
	Seq         uint64            `json:"seq"` // last event seq covered
	LogicalTime int64             `json:"logicalTime"`
	State       string            `json:"state"`
	Vars        map[string][]byte `json:"vars,omitempty"`
	WallTime    time.Time         `json:"wallTime"`
}

// RetentionPolicy drives compaction and reclamation.
type RetentionPolicy struct {
	// KeepSegments is the number of newest sealed segments per instance
	// that are never compacted or reclaimed.
	KeepSegments int
	// ReclaimHorizon is a logical-time distance from the instance's latest
	// logical time; sealed segments entirely older than the horizon (and
	// covered by a checkpoint) may be reclaimed into a summary.
	// Zero disables time-based reclamation.
	ReclaimHorizon int64
	// AutoMaintain runs Maintain after every checkpoint creation.
	AutoMaintain bool
}

// Options configures a Service.
type Options struct {
	// SegmentSize is the number of events per segment before sealing.
	// Default 64.
	SegmentSize int
	// KeepCheckpoints is the number of checkpoint files retained per
	// instance. Default 3.
	KeepCheckpoints int
	// NoFsync skips fsync calls. Only for tests.
	NoFsync bool
}

// CompactReport summarizes a compaction/reclamation run.
type CompactReport struct {
	InstanceID     string
	SegmentsBefore int
	SegmentsAfter  int
	EventsBefore   int
	EventsAfter    int
}

var (
	ErrNotFound         = errors.New("history: instance not found")
	ErrNonMonotonicTime = errors.New("history: logical time must be non-decreasing within an instance")
	ErrHistoryReclaimed = errors.New("history: requested history has been reclaimed")
	ErrNoCheckpoint     = errors.New("history: no checkpoint available")
	ErrClosed           = errors.New("history: service closed")
)
