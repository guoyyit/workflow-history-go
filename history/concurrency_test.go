package history

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestConcurrentMultiInstanceWrites hammers the service from many
// goroutines across several instances and verifies global seq uniqueness
// and per-instance monotonic logical time.
func TestConcurrentMultiInstanceWrites(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 8})
	const instances = 8
	const perInstance = 50

	var wg sync.WaitGroup
	seqs := make([][]uint64, instances)
	for i := 0; i < instances; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			inst := fmt.Sprintf("inst-%d", i)
			for tm := int64(1); tm <= perInstance; tm++ {
				ev, err := s.Commit(inst, Transition{
					LogicalTime: tm,
					To:          fmt.Sprintf("s%d", tm),
					Vars:        map[string][]byte{"n": []byte(fmt.Sprintf("%d", tm))},
				})
				if err != nil {
					t.Errorf("commit: %v", err)
					return
				}
				seqs[i] = append(seqs[i], ev.Seq)
			}
		}(i)
	}
	wg.Wait()

	seen := map[uint64]string{}
	for i := 0; i < instances; i++ {
		if len(seqs[i]) != perInstance {
			t.Fatalf("instance %d committed %d events", i, len(seqs[i]))
		}
		for j, sq := range seqs[i] {
			if _, dup := seen[sq]; dup {
				t.Fatalf("duplicate seq %d", sq)
			}
			seen[sq] = fmt.Sprintf("inst-%d", i)
			if j > 0 && sq <= seqs[i][j-1] {
				t.Fatalf("instance %d seqs not increasing", i)
			}
		}
	}
	if len(seen) != instances*perInstance {
		t.Fatalf("total seqs = %d", len(seen))
	}
	// Every instance's history is fully queryable.
	for i := 0; i < instances; i++ {
		inst := fmt.Sprintf("inst-%d", i)
		for tm := int64(1); tm <= perInstance; tm++ {
			snap, err := s.StateAt(inst, tm)
			if err != nil {
				t.Fatalf("StateAt(%s,%d): %v", inst, tm, err)
			}
			if got := string(snap.Vars["n"]); got != fmt.Sprintf("%d", tm) {
				t.Fatalf("StateAt(%s,%d) = %s", inst, tm, got)
			}
		}
	}
}

// TestConcurrentSameInstanceCommits verifies that concurrent commits to
// the SAME instance stay globally ordered with unique seqs. Writers take
// logical-time tickets from a shared counter; a loser whose ticket went
// stale retries with a fresh one.
func TestConcurrentSameInstanceCommits(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 4})
	const writers = 8
	const perWriter = 25

	var ticket int64
	var mu sync.Mutex
	var seqs []uint64
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < perWriter; k++ {
				for {
					tm := atomic.AddInt64(&ticket, 1)
					ev, err := s.Commit("shared", Transition{LogicalTime: tm, To: fmt.Sprintf("s%d", tm)})
					if err == ErrNonMonotonicTime {
						continue // ticket raced ahead; take a fresh one
					}
					if err != nil {
						t.Errorf("commit: %v", err)
						return
					}
					mu.Lock()
					seqs = append(seqs, ev.Seq)
					mu.Unlock()
					break
				}
			}
		}()
	}
	wg.Wait()

	if len(seqs) != writers*perWriter {
		t.Fatalf("committed %d, want %d", len(seqs), writers*perWriter)
	}
	seen := map[uint64]bool{}
	for _, sq := range seqs {
		if seen[sq] {
			t.Fatalf("duplicate seq %d", sq)
		}
		seen[sq] = true
	}
	// Stored history is ordered and consistent.
	cur, err := s.Current("shared")
	if err != nil {
		t.Fatal(err)
	}
	hist, err := s.History("shared", 0, cur.LogicalTime)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(hist); i++ {
		if hist[i].Seq <= hist[i-1].Seq || hist[i].LogicalTime < hist[i-1].LogicalTime {
			t.Fatalf("history not ordered at %d", i)
		}
	}
}

// TestReadersDuringWrites runs readers concurrently with writers and
// verifies snapshots are always internally consistent (state matches
// vars) and never reflect uncommitted data.
func TestReadersDuringWrites(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 4})
	const total = 200

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap, err := s.Current("a")
				if err == ErrNotFound {
					continue
				}
				if err != nil {
					t.Errorf("Current: %v", err)
					return
				}
				// Consistency: state name and var must agree.
				n := string(snap.Vars["n"])
				if snap.State != "s"+n {
					t.Errorf("inconsistent snapshot: state=%s n=%s", snap.State, n)
					return
				}
				if _, err := s.StateAt("a", snap.LogicalTime); err != nil {
					t.Errorf("StateAt: %v", err)
					return
				}
			}
		}()
	}
	for tm := int64(1); tm <= total; tm++ {
		if _, err := s.Commit("a", Transition{
			LogicalTime: tm,
			To:          fmt.Sprintf("s%d", tm),
			Vars:        map[string][]byte{"n": []byte(fmt.Sprintf("%d", tm))},
		}); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	close(stop)
	wg.Wait()
}

// TestCheckpointConcurrentWithCommits ensures checkpoint creation never
// captures a half-committed state.
func TestCheckpointConcurrentWithCommits(t *testing.T) {
	s := openTest(t, Options{SegmentSize: 4})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for tm := int64(1); tm <= 100; tm++ {
			if _, err := s.Commit("a", Transition{
				LogicalTime: tm,
				To:          fmt.Sprintf("s%d", tm),
				Vars:        map[string][]byte{"n": []byte(fmt.Sprintf("%d", tm))},
			}); err != nil {
				t.Errorf("commit: %v", err)
				return
			}
		}
	}()
	for i := 0; i < 10; i++ {
		ck, err := s.CreateCheckpoint("a")
		if err == ErrNotFound {
			continue
		}
		if err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
		if ck.State != "s"+string(ck.Vars["n"]) {
			t.Fatalf("checkpoint inconsistent: %+v", ck)
		}
	}
	wg.Wait()
}
