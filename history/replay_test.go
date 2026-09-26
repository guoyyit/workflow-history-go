package history

import (
	"errors"
	"fmt"
	"testing"
)

func collectSteps(t *testing.T, rs *ReplaySession) []ReplayStep {
	t.Helper()
	var out []ReplayStep
	for {
		step, ok := rs.Next()
		if !ok {
			return out
		}
		out = append(out, step)
	}
}

func TestReplayIsDeterministic(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 3})
	commitN(t, s, "a", 1, 10)
	ck, err := s.CreateCheckpoint("a")
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, s, "a", 11, 20)

	run := func() []ReplayStep {
		rs, err := s.StartReplay("a", ReplayOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return collectSteps(t, rs)
	}
	r1, r2 := run(), run()
	if len(r1) != 10 { // events 11..20 after checkpoint at seq 10
		t.Fatalf("steps = %d, want 10", len(r1))
	}
	for i := range r1 {
		if r1[i].Event.Seq != r2[i].Event.Seq ||
			r1[i].State.State != r2[i].State.State ||
			string(r1[i].State.Vars["n"]) != string(r2[i].State.Vars["n"]) {
			t.Fatalf("step %d differs between runs", i)
		}
		want := fmt.Sprintf("%d", 11+i)
		if string(r1[i].State.Vars["n"]) != want {
			t.Fatalf("step %d state n=%s, want %s", i, r1[i].State.Vars["n"], want)
		}
	}
	_ = ck
}

func TestReplayResumeAfterInterrupt(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 3})
	commitN(t, s, "a", 1, 8)
	if _, err := s.CreateCheckpoint("a"); err != nil {
		t.Fatal(err)
	}
	commitN(t, s, "a", 9, 18)

	// Full run for reference.
	full, err := s.StartReplay("a", ReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	reference := collectSteps(t, full)

	// Interrupted run: stop after 4 steps, resume via cursor.
	part1, err := s.StartReplay("a", ReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var got []ReplayStep
	for i := 0; i < 4; i++ {
		step, ok := part1.Next()
		if !ok {
			t.Fatal("unexpected end")
		}
		got = append(got, step)
	}
	cursor := part1.Cursor()

	part2, err := s.StartReplay("a", ReplayOptions{AfterSeq: cursor})
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, collectSteps(t, part2)...)

	if len(got) != len(reference) {
		t.Fatalf("resumed replay has %d steps, want %d", len(got), len(reference))
	}
	for i := range reference {
		if got[i].Event.Seq != reference[i].Event.Seq ||
			got[i].State.State != reference[i].State.State ||
			string(got[i].State.Vars["n"]) != string(reference[i].State.Vars["n"]) {
			t.Fatalf("step %d mismatch after resume", i)
		}
	}
}

func TestReplayFromSpecificCheckpoint(t *testing.T) {
	s := openTest(t, Options{KeepCheckpoints: 5})
	commitN(t, s, "a", 1, 5)
	ck1, err := s.CreateCheckpoint("a")
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, s, "a", 6, 9)
	if _, err := s.CreateCheckpoint("a"); err != nil {
		t.Fatal(err)
	}
	rs, err := s.StartReplay("a", ReplayOptions{CheckpointSeq: ck1.Seq})
	if err != nil {
		t.Fatal(err)
	}
	steps := collectSteps(t, rs)
	if len(steps) != 4 { // events 6..9
		t.Fatalf("steps = %d, want 4", len(steps))
	}
	if steps[0].Event.LogicalTime != 6 {
		t.Fatalf("first step time = %d, want 6", steps[0].Event.LogicalTime)
	}
}

func TestReplayRequiresCheckpoint(t *testing.T) {
	s := openTest(t, Options{})
	commitN(t, s, "a", 1, 3)
	if _, err := s.StartReplay("a", ReplayOptions{}); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("want ErrNoCheckpoint, got %v", err)
	}
	if _, err := s.StartReplay("ghost", ReplayOptions{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestReplayUnaffectedByCompaction(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 2})
	s.SetRetentionPolicy(RetentionPolicy{KeepSegments: 1})
	commitN(t, s, "a", 1, 6)
	if _, err := s.CreateCheckpoint("a"); err != nil { // covers 1..6
		t.Fatal(err)
	}
	commitN(t, s, "a", 7, 12)
	ck2, err := s.CreateCheckpoint("a") // covers 1..12
	if err != nil {
		t.Fatal(err)
	}
	commitN(t, s, "a", 13, 16)

	replayNow := func() []ReplayStep {
		rs, err := s.StartReplay("a", ReplayOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return collectSteps(t, rs)
	}
	before := replayNow()

	// Compact + reclaim the checkpoint-covered prefix.
	if _, err := s.Compact("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Reclaim("a", 10); err != nil {
		t.Fatal(err)
	}

	after := replayNow()
	if len(before) != len(after) {
		t.Fatalf("replay length changed: %d -> %d", len(before), len(after))
	}
	for i := range before {
		if before[i].Event.Seq != after[i].Event.Seq ||
			string(before[i].State.Vars["n"]) != string(after[i].State.Vars["n"]) {
			t.Fatalf("replay step %d changed by compaction", i)
		}
	}
	_ = ck2
}

func TestReplaySeesStableSnapshot(t *testing.T) {
	s := openTest(t, Options{})
	commitN(t, s, "a", 1, 5)
	if _, err := s.CreateCheckpoint("a"); err != nil {
		t.Fatal(err)
	}
	commitN(t, s, "a", 6, 8)
	rs, err := s.StartReplay("a", ReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Concurrent commits during an in-flight replay must not disturb it.
	commitN(t, s, "a", 9, 12)
	steps := collectSteps(t, rs)
	if len(steps) != 3 { // only events 6..8 as of session start
		t.Fatalf("in-flight replay disturbed: %d steps, want 3", len(steps))
	}
}
