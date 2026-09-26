package history_test

import (
	"fmt"
	"os"

	"github.com/guoyyit/workflow-history-go/history"
)

func Example() {
	dir, err := os.MkdirTemp("", "example-history")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	s, err := history.Open(dir, history.Options{SegmentSize: 4})
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Record state transitions with variable snapshots.
	for tm := int64(1); tm <= 5; tm++ {
		_, err := s.Commit("order-42", history.Transition{
			LogicalTime: tm,
			From:        "pending",
			To:          fmt.Sprintf("step-%d", tm),
			Vars:        map[string][]byte{"progress": []byte(fmt.Sprintf("%d%%", tm*20))},
		})
		if err != nil {
			panic(err)
		}
	}

	// Point-in-time query.
	snap, _ := s.StateAt("order-42", 3)
	fmt.Printf("state at t=3: %s, progress=%s\n", snap.State, snap.Vars["progress"])

	// Checkpoint, then replay everything after it.
	ck, _ := s.CreateCheckpoint("order-42")
	rs, _ := s.StartReplay("order-42", history.ReplayOptions{})
	for {
		step, ok := rs.Next()
		if !ok {
			break
		}
		fmt.Printf("replay seq=%d state=%s\n", step.Event.Seq, step.State.State)
	}
	fmt.Printf("checkpoint covered seq=%d\n", ck.Seq)

	// Output:
	// state at t=3: step-3, progress=60%
	// checkpoint covered seq=5
}
