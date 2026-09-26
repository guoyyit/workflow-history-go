package history

import (
	"errors"
	"testing"
)

// TestCommitAssignsMonotonicSeqAndLogicalTime 验证提交后事件的
// 全局序号与实例内逻辑时间均单调递增。
func TestCommitAssignsMonotonicSeqAndLogicalTime(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	st := svc.Store()

	var lastSeq uint64
	for i := 0; i < 5; i++ {
		ev, err := st.Commit("inst-a", Transition{From: "s", To: "s", Vars: map[string]any{"i": i}})
		if err != nil {
			t.Fatal(err)
		}
		if ev.Seq <= lastSeq && i > 0 {
			t.Fatalf("seq not increasing: %d after %d", ev.Seq, lastSeq)
		}
		lastSeq = ev.Seq
		if ev.LogicalTime != uint64(i+1) {
			t.Fatalf("logical time = %d, want %d", ev.LogicalTime, i+1)
		}
	}
}

// TestQueryAtTimeTravel 验证按逻辑时间查询只反映该时间点之前已提交的状态。
func TestQueryAtTimeTravel(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	st := svc.Store()

	states := []string{"created", "running", "blocked", "running", "done"}
	for i, to := range states {
		if _, err := st.Commit("wf", Transition{From: "x", To: to, Vars: map[string]any{"step": i}}); err != nil {
			t.Fatal(err)
		}
	}

	// 查询逻辑时间 2：应看到第 2 次迁移后的状态，不得混入更晚写入。
	snap, err := st.QueryAt("wf", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Exact || snap.State != "running" || snap.Vars["step"] != 1 {
		t.Fatalf("unexpected snapshot at t=2: %+v", snap)
	}
	// 查询远超当前的时间：返回最新已提交状态。
	snap, err = st.QueryAt("wf", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if snap.State != "done" || snap.Vars["step"] != 4 {
		t.Fatalf("unexpected snapshot at t=1000: %+v", snap)
	}
	// 查询逻辑时间 0（任何事件之前）：不存在。
	if _, err := st.QueryAt("wf", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("query at t=0: err=%v, want ErrNotFound", err)
	}
	// 不存在的实例。
	if _, err := st.QueryAt("ghost", 5); !errors.Is(err, ErrNotFound) {
		t.Fatalf("query ghost: err=%v, want ErrNotFound", err)
	}
}

// TestQueryResultIsolation 验证查询结果是拷贝，修改不影响存储内状态。
func TestQueryResultIsolation(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	st := svc.Store()

	if _, err := st.Commit("wf", Transition{To: "s1", Vars: map[string]any{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	snap, err := st.QueryAt("wf", 1)
	if err != nil {
		t.Fatal(err)
	}
	snap.Vars["k"] = "mutated"
	snap2, _ := st.QueryAt("wf", 1)
	if snap2.Vars["k"] != "v" {
		t.Fatalf("stored vars mutated via query result: %v", snap2.Vars["k"])
	}
}

// TestCheckpointLifecycle 验证检查点的保存与读取。
func TestCheckpointLifecycle(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	st := svc.Store()

	// 无历史时无法建立检查点。
	if _, err := st.SaveCheckpoint("wf"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("checkpoint without history: err=%v", err)
	}
	st.Commit("wf", Transition{To: "s1", Vars: map[string]any{"a": 1}})
	st.Commit("wf", Transition{To: "s2", Vars: map[string]any{"a": 2}})
	cp, err := st.SaveCheckpoint("wf")
	if err != nil {
		t.Fatal(err)
	}
	if cp.LogicalTime != 2 || cp.State != "s2" || cp.Vars["a"] != 2 {
		t.Fatalf("unexpected checkpoint: %+v", cp)
	}
	got, err := st.LatestCheckpoint("wf")
	if err != nil {
		t.Fatal(err)
	}
	if got.LogicalTime != cp.LogicalTime || got.Seq != cp.Seq {
		t.Fatalf("latest checkpoint mismatch: %+v vs %+v", got, cp)
	}
}
