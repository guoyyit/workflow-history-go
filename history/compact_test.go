package history

import (
	"errors"
	"fmt"
	"testing"
)

// commitWithDups commits count transitions, several per logical tick,
// so exact compaction has duplicates to remove.
func commitWithDups(t *testing.T, s *Service, inst string, ticks int, perTick int) {
	t.Helper()
	for tm := 1; tm <= ticks; tm++ {
		for k := 0; k < perTick; k++ {
			_, err := s.Commit(inst, Transition{
				LogicalTime: int64(tm),
				To:          fmt.Sprintf("s%d.%d", tm, k),
				Vars:        map[string][]byte{"n": []byte(fmt.Sprintf("%d.%d", tm, k))},
			})
			if err != nil {
				t.Fatalf("Commit: %v", err)
			}
		}
	}
}

func snapshotMap(t *testing.T, s *Service, inst string, maxTime int64) map[int64]Snapshot {
	t.Helper()
	out := map[int64]Snapshot{}
	for tm := int64(0); tm <= maxTime+1; tm++ {
		snap, err := s.StateAt(inst, tm)
		if err != nil {
			t.Fatalf("StateAt(%d): %v", tm, err)
		}
		out[tm] = snap
	}
	return out
}

func TestCompactIsExact(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 4})
	s.SetRetentionPolicy(RetentionPolicy{KeepSegments: 1})
	commitWithDups(t, s, "a", 10, 3) // 30 events, 3 per tick
	before := snapshotMap(t, s, "a", 10)
	if _, err := s.CreateCheckpoint("a"); err != nil {
		t.Fatal(err)
	}
	rep, err := s.Compact("a")
	if err != nil {
		t.Fatal(err)
	}
	if rep.EventsAfter >= rep.EventsBefore {
		t.Fatalf("compact did not shrink: %+v", rep)
	}
	after := snapshotMap(t, s, "a", 10)
	for tm, want := range before {
		got := after[tm]
		if got.Exact != want.Exact {
			t.Fatalf("StateAt(%d) exactness changed: before=%+v after=%+v", tm, want, got)
		}
		if !want.Exact {
			continue // inexact boundary fallback may move with retention
		}
		if got.State != want.State || got.Seq != want.Seq ||
			string(got.Vars["n"]) != string(want.Vars["n"]) {
			t.Fatalf("StateAt(%d) changed by compaction: before=%+v after=%+v", tm, want, got)
		}
	}
}

func TestCompactRequiresCheckpoint(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 2})
	commitN(t, s, "a", 1, 6)
	if _, err := s.Compact("a"); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("want ErrNoCheckpoint, got %v", err)
	}
	if _, err := s.Reclaim("a", 4); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("want ErrNoCheckpoint, got %v", err)
	}
}

func TestCompactNeverTouchesUncoveredEvents(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 2})
	s.SetRetentionPolicy(RetentionPolicy{KeepSegments: 0})
	commitN(t, s, "a", 1, 6)
	if _, err := s.CreateCheckpoint("a"); err != nil { // covers seq 6
		t.Fatal(err)
	}
	commitN(t, s, "a", 7, 10) // NOT covered by any checkpoint
	before := snapshotMap(t, s, "a", 10)
	if _, err := s.Compact("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Reclaim("a", 100); err != nil { // horizon beyond everything
		t.Fatal(err)
	}
	after := snapshotMap(t, s, "a", 10)
	// Everything after the checkpoint (t=7..10) must be fully intact.
	for tm := int64(7); tm <= 10; tm++ {
		if !after[tm].Exact || after[tm].Seq != before[tm].Seq {
			t.Fatalf("uncovered event at %d lost: %+v", tm, after[tm])
		}
	}
	// Covered prefix may be summarized, but the summary boundary must be
	// the checkpoint position and queries stay well defined.
	snap := after[1]
	if snap.Exact {
		t.Fatalf("covered prefix should be summarized, got exact: %+v", snap)
	}
	if snap.Seq != 6 || string(snap.Vars["n"]) != "6" {
		t.Fatalf("summary boundary = %+v, want state at t=6", snap)
	}
}

func TestReclaimKeepsBoundarySummary(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 2})
	s.SetRetentionPolicy(RetentionPolicy{KeepSegments: 1})
	commitN(t, s, "a", 1, 12)
	if _, err := s.CreateCheckpoint("a"); err != nil {
		t.Fatal(err)
	}
	rep, err := s.Reclaim("a", 8)
	if err != nil {
		t.Fatal(err)
	}
	if rep.EventsAfter != 1 {
		t.Fatalf("reclaim report = %+v", rep)
	}
	// t=9..12 exact.
	for tm := int64(9); tm <= 12; tm++ {
		snap, err := s.StateAt("a", tm)
		if err != nil || !snap.Exact || string(snap.Vars["n"]) != fmt.Sprintf("%d", tm) {
			t.Fatalf("StateAt(%d) = %+v, %v", tm, snap, err)
		}
	}
	// t<8 falls to the boundary summary at t=8.
	snap, err := s.StateAt("a", 5)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Exact || snap.Seq != 8 || string(snap.Vars["n"]) != "8" {
		t.Fatalf("boundary = %+v", snap)
	}
	// Monotonicity still enforced against the summary boundary.
	if _, err := s.Commit("a", Transition{LogicalTime: 7, To: "x"}); !errors.Is(err, ErrNonMonotonicTime) {
		t.Fatalf("want ErrNonMonotonicTime, got %v", err)
	}
}

func TestKeepSegmentsProtectsNewest(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 2})
	s.SetRetentionPolicy(RetentionPolicy{KeepSegments: 2})
	commitN(t, s, "a", 1, 8) // segments [1,2][3,4][5,6][7,8]
	if _, err := s.CreateCheckpoint("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Reclaim("a", 100); err != nil {
		t.Fatal(err)
	}
	// Newest 2 segments ([5,6],[7,8]) survive untouched.
	for tm := int64(5); tm <= 8; tm++ {
		snap, err := s.StateAt("a", tm)
		if err != nil || !snap.Exact {
			t.Fatalf("StateAt(%d) = %+v, %v", tm, snap, err)
		}
	}
	// Older prefix summarized at t=4.
	snap, err := s.StateAt("a", 1)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Exact || string(snap.Vars["n"]) != "4" {
		t.Fatalf("prefix = %+v", snap)
	}
}

func TestMaintainAppliesPolicy(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 2})
	s.SetRetentionPolicy(RetentionPolicy{KeepSegments: 1, ReclaimHorizon: 4, AutoMaintain: true})
	commitN(t, s, "a", 1, 12)
	if _, err := s.CreateCheckpoint("a"); err != nil { // triggers AutoMaintain
		t.Fatal(err)
	}
	// Horizon = lastTime(12) - 4 = 8: segments <= 8 reclaimed.
	snap, err := s.StateAt("a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Exact || string(snap.Vars["n"]) != "8" {
		t.Fatalf("auto-maintained prefix = %+v", snap)
	}
	for tm := int64(9); tm <= 12; tm++ {
		snap, err := s.StateAt("a", tm)
		if err != nil || !snap.Exact {
			t.Fatalf("StateAt(%d) = %+v, %v", tm, snap, err)
		}
	}
}
