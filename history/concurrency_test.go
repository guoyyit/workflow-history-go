package history

import (
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentCommitGlobalOrder 验证多实例并发提交时：
//   - 全局序号不重复；
//   - 每个实例内逻辑时间从 1 开始连续递增；
//   - 已提交事件总数与提交成功数一致。
func TestConcurrentCommitGlobalOrder(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	st := svc.Store()

	const instances = 8
	const perInstance = 50

	var wg sync.WaitGroup
	seqCh := make(chan uint64, instances*perInstance)
	ltCh := make(map[string]chan uint64)
	for i := 0; i < instances; i++ {
		ltCh[fmt.Sprintf("inst-%d", i)] = make(chan uint64, perInstance)
	}
	for i := 0; i < instances; i++ {
		id := fmt.Sprintf("inst-%d", i)
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for j := 0; j < perInstance; j++ {
				ev, err := st.Commit(id, Transition{To: "s", Vars: map[string]any{"j": j}})
				if err != nil {
					t.Errorf("commit: %v", err)
					return
				}
				seqCh <- ev.Seq
				ltCh[id] <- ev.LogicalTime
			}
		}(id)
	}
	wg.Wait()
	close(seqCh)

	// 全局序号唯一。
	seen := make(map[uint64]bool)
	count := 0
	for s := range seqCh {
		if seen[s] {
			t.Fatalf("duplicate global seq %d", s)
		}
		seen[s] = true
		count++
	}
	if count != instances*perInstance {
		t.Fatalf("committed %d events, want %d", count, instances*perInstance)
	}
	// 实例内逻辑时间连续递增。
	for id, ch := range ltCh {
		close(ch)
		want := uint64(1)
		for lt := range ch {
			if lt != want {
				t.Fatalf("%s: logical time %d, want %d", id, lt, want)
			}
			want++
		}
		if want != perInstance+1 {
			t.Fatalf("%s: %d events, want %d", id, want-1, perInstance)
		}
	}
}

// TestConcurrentReadWriteConsistency 验证读写并发时，读请求总能观察到
// 一致的已提交快照：查询逻辑时间 t 的结果，其命中事件的逻辑时间
// 不超过 t，且变量值与某一真实提交边界一致。
func TestConcurrentReadWriteConsistency(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	st := svc.Store()

	const writers = 4
	const commits = 100

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < writers; w++ {
		id := fmt.Sprintf("rw-%d", w)
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for j := 0; j < commits; j++ {
				if _, err := st.Commit(id, Transition{To: "s", Vars: map[string]any{"j": j}}); err != nil {
					t.Errorf("commit: %v", err)
					return
				}
			}
		}(id)
	}
	// 并发读者：不阻塞写，且结果必须自洽（Vars.j == LogicalTime-1）。
	var rwg sync.WaitGroup
	for r := 0; r < 4; r++ {
		rwg.Add(1)
		go func(r int) {
			defer rwg.Done()
			id := fmt.Sprintf("rw-%d", r%writers)
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap, err := st.QueryAt(id, commits)
				if err != nil {
					continue // 实例可能还没有已提交事件
				}
				j, ok := snap.Vars["j"].(int)
				if !ok || uint64(j) != snap.LogicalTime-1 {
					t.Errorf("inconsistent snapshot: lt=%d j=%v", snap.LogicalTime, snap.Vars["j"])
					return
				}
			}
		}(r)
	}
	wg.Wait()
	close(stop)
	rwg.Wait()
}
