package history

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// reopen simulates a crash + restart: the old service is abandoned
// without Close (every commit is already durable) and a new service
// is opened on the same directory.
func reopen(t *testing.T, s *Service) *Service {
	t.Helper()
	ns, err := Open(s.dir, s.opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { ns.Close() })
	return ns
}

func TestRecoverySeesCommittedHistory(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, s, "a", 1, 10)
	commitN(t, s, "b", 1, 6)
	if _, err := s.CreateCheckpoint("a"); err != nil {
		t.Fatal(err)
	}
	commitN(t, s, "a", 11, 13) // events after the checkpoint

	ns := reopen(t, s)
	// Full history of both instances must be visible.
	for tm := int64(1); tm <= 13; tm++ {
		snap, err := ns.StateAt("a", tm)
		if err != nil {
			t.Fatalf("StateAt(a,%d) after recovery: %v", tm, err)
		}
		if got := string(snap.Vars["n"]); got != fmt.Sprintf("%d", tm) {
			t.Fatalf("StateAt(a,%d) n=%s", tm, got)
		}
	}
	for tm := int64(1); tm <= 6; tm++ {
		if _, err := ns.StateAt("b", tm); err != nil {
			t.Fatalf("StateAt(b,%d) after recovery: %v", tm, err)
		}
	}
	// Global sequence continues without reuse.
	ev, err := ns.Commit("a", Transition{LogicalTime: 14, To: "s14"})
	if err != nil {
		t.Fatal(err)
	}
	if ev.Seq != 20 { // 13 (a) + 6 (b) committed, next global seq is 20
		t.Fatalf("seq after recovery = %d, want 20", ev.Seq)
	}
	// Checkpoint survives.
	ck, ok, err := ns.LatestCheckpoint("a")
	if err != nil || !ok {
		t.Fatalf("checkpoint after recovery: ok=%v err=%v", ok, err)
	}
	if ck.Seq != 10 || ck.LogicalTime != 10 {
		t.Fatalf("checkpoint = %+v", ck)
	}
	s.Close()
}

func TestRecoverySeqNeverReused(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	evs := commitN(t, s, "a", 1, 9)
	last := evs[len(evs)-1].Seq
	ns := reopen(t, s)
	ev, err := ns.Commit("a", Transition{LogicalTime: 10, To: "s10"})
	if err != nil {
		t.Fatal(err)
	}
	if ev.Seq != last+1 {
		t.Fatalf("seq reused: got %d, want %d", ev.Seq, last+1)
	}
	s.Close()
}

func TestRecoveryIgnoresTornTail(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, s, "a", 1, 5)
	s.Close() // clean close; WAL has 5 valid records

	// Simulate a torn write: append garbage to the WAL.
	f, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	ns, err := Open(dir, Options{SegmentSize: 100})
	if err != nil {
		t.Fatalf("open with torn tail: %v", err)
	}
	defer ns.Close()
	for tm := int64(1); tm <= 5; tm++ {
		if _, err := ns.StateAt("a", tm); err != nil {
			t.Fatalf("StateAt(%d) after torn tail: %v", tm, err)
		}
	}
	// Service keeps working and the garbage is gone.
	if _, err := ns.Commit("a", Transition{LogicalTime: 6, To: "s6"}); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	batches, validLen, err := readWALBatches(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != validLen {
		t.Fatalf("wal size %d != valid length %d", st.Size(), validLen)
	}
	if len(batches) != 6 {
		t.Fatalf("batches = %d, want 6", len(batches))
	}
}

func TestRecoveryIgnoresUnconfirmedWrite(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, s, "a", 1, 3)
	s.Close()

	// Forge a half-written record: valid header, truncated payload.
	rec, err := encodeWALRecord([]Event{{Seq: 99, InstanceID: "a", LogicalTime: 4, To: "ghost"}})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(rec[:len(rec)-5]); err != nil { // truncated
		t.Fatal(err)
	}
	f.Close()

	ns, err := Open(dir, Options{SegmentSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer ns.Close()
	if _, err := ns.StateAt("a", 4); err == nil {
		// t=4 must not resolve to the forged event; state stays at t=3.
		snap, _ := ns.StateAt("a", 4)
		if snap.State == "ghost" {
			t.Fatal("unconfirmed write became visible")
		}
	}
	cur, err := ns.Current("a")
	if err != nil {
		t.Fatal(err)
	}
	if cur.State != "s3" {
		t.Fatalf("current state = %s, want s3", cur.State)
	}
}

func TestRecoveryAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	s.SetRetentionPolicy(RetentionPolicy{KeepSegments: 1})
	commitN(t, s, "a", 1, 12)
	if _, err := s.CreateCheckpoint("a"); err != nil {
		t.Fatal(err)
	}
	// Reclaim segments entirely <= t=8 (segments are [1,2]..[11,12]).
	if _, err := s.Reclaim("a", 8); err != nil {
		t.Fatal(err)
	}

	ns := reopen(t, s)
	// Post-reclaim queries still work after restart.
	snap, err := ns.StateAt("a", 12)
	if err != nil || string(snap.Vars["n"]) != "12" {
		t.Fatalf("StateAt(12) = %v, %v", snap, err)
	}
	for tm := int64(8); tm <= 12; tm++ {
		snap, err := ns.StateAt("a", tm)
		if err != nil || !snap.Exact || string(snap.Vars["n"]) != fmt.Sprintf("%d", tm) {
			t.Fatalf("StateAt(%d) = %+v, %v", tm, snap, err)
		}
	}
	snap, err = ns.StateAt("a", 3)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Exact {
		t.Fatal("reclaimed range must report Exact=false")
	}
	if string(snap.Vars["n"]) != "8" { // boundary summary state
		t.Fatalf("boundary summary n=%s, want 8", snap.Vars["n"])
	}
	s.Close()
}
