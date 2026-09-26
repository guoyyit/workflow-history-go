package history

import (
	"errors"
	"fmt"
	"testing"
)

func openTest(t *testing.T, opts Options) *Service {
	t.Helper()
	s, err := Open(t.TempDir(), opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func commitN(t *testing.T, s *Service, inst string, from, to int64) []Event {
	t.Helper()
	var out []Event
	for tm := from; tm <= to; tm++ {
		ev, err := s.Commit(inst, Transition{
			LogicalTime: tm,
			From:        fmt.Sprintf("s%d", tm-1),
			To:          fmt.Sprintf("s%d", tm),
			Vars:        map[string][]byte{"n": []byte(fmt.Sprintf("%d", tm))},
		})
		if err != nil {
			t.Fatalf("Commit t=%d: %v", tm, err)
		}
		out = append(out, ev)
	}
	return out
}

func TestCommitAssignsMonotonicUniqueSeqs(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 4})
	evs := commitN(t, s, "a", 1, 10)
	for i, ev := range evs {
		if ev.Seq != uint64(i+1) {
			t.Fatalf("event %d seq = %d, want %d", i, ev.Seq, i+1)
		}
	}
	// Sequences are global across instances.
	ev, err := s.Commit("b", Transition{LogicalTime: 1, To: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if ev.Seq != 11 {
		t.Fatalf("cross-instance seq = %d, want 11", ev.Seq)
	}
}

func TestCommitRejectsNonMonotonicTime(t *testing.T) {
	s := openTest(t, Options{})
	commitN(t, s, "a", 5, 5)
	if _, err := s.Commit("a", Transition{LogicalTime: 4, To: "x"}); !errors.Is(err, ErrNonMonotonicTime) {
		t.Fatalf("want ErrNonMonotonicTime, got %v", err)
	}
	// Equal time is allowed (multiple transitions per tick).
	if _, err := s.Commit("a", Transition{LogicalTime: 5, To: "y"}); err != nil {
		t.Fatalf("equal time must be allowed: %v", err)
	}
}

func TestStateAtPointInTime(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 3})
	commitN(t, s, "a", 1, 10)
	for tm := int64(1); tm <= 10; tm++ {
		snap, err := s.StateAt("a", tm)
		if err != nil {
			t.Fatalf("StateAt(%d): %v", tm, err)
		}
		if !snap.Exact {
			t.Fatalf("StateAt(%d) not exact", tm)
		}
		if got := string(snap.Vars["n"]); got != fmt.Sprintf("%d", tm) {
			t.Fatalf("StateAt(%d) n=%s", tm, got)
		}
		if snap.State != fmt.Sprintf("s%d", tm) {
			t.Fatalf("StateAt(%d) state=%s", tm, snap.State)
		}
	}
	// Between ticks: state of the latest event <= t.
	snap, err := s.StateAt("a", 7)
	if err != nil || string(snap.Vars["n"]) != "7" {
		t.Fatalf("StateAt(7) = %v, %v", snap, err)
	}
	// Far future: latest state.
	snap, err = s.StateAt("a", 1000)
	if err != nil || string(snap.Vars["n"]) != "10" {
		t.Fatalf("StateAt(1000) = %v, %v", snap, err)
	}
}

func TestStateAtUnknownInstance(t *testing.T) {
	s := openTest(t, Options{})
	if _, err := s.StateAt("nope", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestHistoryRange(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 2})
	commitN(t, s, "a", 1, 10)
	evs, err := s.History("a", 3, 6)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 4 {
		t.Fatalf("History(3,6) len=%d, want 4", len(evs))
	}
	for i, ev := range evs {
		if ev.LogicalTime != int64(i+3) {
			t.Fatalf("ev[%d].LogicalTime=%d", i, ev.LogicalTime)
		}
	}
}

func TestCurrentSnapshot(t *testing.T) {
	s := openTest(t, Options{})
	commitN(t, s, "a", 1, 5)
	cur, err := s.Current("a")
	if err != nil {
		t.Fatal(err)
	}
	if cur.State != "s5" || string(cur.Vars["n"]) != "5" || !cur.Exact {
		t.Fatalf("Current = %+v", cur)
	}
}

func TestReturnedSnapshotsAreCopies(t *testing.T) {
	s := openTest(t, Options{})
	commitN(t, s, "a", 1, 3)
	snap, err := s.StateAt("a", 2)
	if err != nil {
		t.Fatal(err)
	}
	snap.Vars["n"][0] = 'X'
	snap.Vars["injected"] = []byte("bad")
	again, err := s.StateAt("a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if string(again.Vars["n"]) != "2" {
		t.Fatal("stored vars mutated through returned snapshot")
	}
	if _, ok := again.Vars["injected"]; ok {
		t.Fatal("injected key visible in stored vars")
	}
}

func TestCheckpointLifecycle(t *testing.T) {
	s := openTest(t, Options{KeepCheckpoints: 2})
	commitN(t, s, "a", 1, 5)
	ck, err := s.CreateCheckpoint("a")
	if err != nil {
		t.Fatal(err)
	}
	if ck.Seq != 5 || ck.LogicalTime != 5 || ck.State != "s5" {
		t.Fatalf("checkpoint = %+v", ck)
	}
	got, ok, err := s.LatestCheckpoint("a")
	if err != nil || !ok {
		t.Fatalf("LatestCheckpoint ok=%v err=%v", ok, err)
	}
	if got.Seq != ck.Seq {
		t.Fatalf("LatestCheckpoint seq=%d want %d", got.Seq, ck.Seq)
	}
	// Pruning keeps only KeepCheckpoints files.
	commitN(t, s, "a", 6, 7)
	if _, err := s.CreateCheckpoint("a"); err != nil {
		t.Fatal(err)
	}
	commitN(t, s, "a", 8, 9)
	if _, err := s.CreateCheckpoint("a"); err != nil {
		t.Fatal(err)
	}
	files, err := listCheckpointFiles(s.dir, "a")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("checkpoint files = %d, want 2", len(files))
	}
	// Checkpoint of unknown instance.
	if _, err := s.CreateCheckpoint("ghost"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
