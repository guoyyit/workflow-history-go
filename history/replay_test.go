package history

import (
	"reflect"
	"testing"
)

// replayState 是测试用的回放状态：累计应用的事件数与变量 i 的和。
type replayState struct {
	Count int
	Sum   int
	Last  string
}

// step 是确定性纯函数：仅由输入决定输出。
func step(state any, ev Event) (any, error) {
	st := state.(replayState)
	st.Count++
	st.Last = ev.To
	if i, ok := ev.Vars["i"].(int); ok {
		st.Sum += i
	} else if f, ok := ev.Vars["i"].(float64); ok { // 恢复后为 float64
		st.Sum += int(f)
	}
	return st, nil
}

// runAll 从会话一路推进到追平历史，返回最终状态。
func runAll(t *testing.T, rs *ReplaySession) replayState {
	t.Helper()
	for {
		ok, err := rs.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
	}
	_, st := rs.State()
	return st.(replayState)
}

// setupReplayFixture 提交 10 条事件，在逻辑时间 4 处建立检查点，
// 之后再提交 6 条（变量 i 从 100 起，便于区分）。
func setupReplayFixture(t *testing.T, st *Store, id string) Checkpoint {
	t.Helper()
	commitN(t, st, id, 4)
	cp, err := st.SaveCheckpoint(id)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if _, err := st.Commit(id, Transition{To: "s", Vars: map[string]any{"i": 100 + i}}); err != nil {
			t.Fatal(err)
		}
	}
	return cp
}

// TestReplayDeterministic 验证同一历史与相同回放入口产生完全一致的结果。
func TestReplayDeterministic(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	st := svc.Store()
	setupReplayFixture(t, st, "wf")

	rs1, err := st.StartReplay("wf", replayState{}, step)
	if err != nil {
		t.Fatal(err)
	}
	got1 := runAll(t, rs1)

	rs2, err := st.StartReplay("wf", replayState{}, step)
	if err != nil {
		t.Fatal(err)
	}
	got2 := runAll(t, rs2)

	if !reflect.DeepEqual(got1, got2) {
		t.Fatalf("non-deterministic replay: %+v vs %+v", got1, got2)
	}
	want := replayState{Count: 6, Sum: 100 + 101 + 102 + 103 + 104 + 105, Last: "s"}
	if got1 != want {
		t.Fatalf("replay = %+v, want %+v", got1, want)
	}
}

// TestReplayInterruptResume 验证回放中断后从游标恢复，
// 最终结果与不中断回放完全一致。
func TestReplayInterruptResume(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	st := svc.Store()
	setupReplayFixture(t, st, "wf")

	// 不中断的完整回放作为基准。
	base, err := st.StartReplay("wf", replayState{}, step)
	if err != nil {
		t.Fatal(err)
	}
	want := runAll(t, base)

	// 中断回放：只推进 2 步。
	interrupted, err := st.StartReplay("wf", replayState{}, step)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := interrupted.Next(); err != nil {
			t.Fatal(err)
		}
	}
	cursor, state := interrupted.State()

	// 从断点恢复并推进到底。
	resumed, err := st.ResumeReplay("wf", cursor, state, step)
	if err != nil {
		t.Fatal(err)
	}
	got := runAll(t, resumed)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resumed replay = %+v, want %+v", got, want)
	}
}

// TestReplayAfterCrashRecovery 验证崩溃恢复后仍能从最近一次
// 已提交检查点回放，且结果与崩溃前一致。
func TestReplayAfterCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	st := svc.Store()
	setupReplayFixture(t, st, "wf")

	// 崩溃前的回放结果作为基准。
	rs, err := st.StartReplay("wf", replayState{}, step)
	if err != nil {
		t.Fatal(err)
	}
	want := runAll(t, rs)

	// 模拟崩溃：撕裂的尾部写入 + 重开。
	svc2 := simulateCrash(t, dir, []byte{0x57, 0x48, 0x4c, 0x47, 0x01})
	defer svc2.Close()
	st2 := svc2.Store()

	cp, err := st2.LatestCheckpoint("wf")
	if err != nil {
		t.Fatal(err)
	}
	if cp.LogicalTime != 4 {
		t.Fatalf("checkpoint after recovery: %+v", cp)
	}
	rs2, err := st2.StartReplay("wf", replayState{}, step)
	if err != nil {
		t.Fatal(err)
	}
	got := runAll(t, rs2)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replay after recovery = %+v, want %+v", got, want)
	}
}

// TestReplayAfterCompaction 验证压缩不破坏检查点之后的回放：
// 被压缩的是检查点之前的事件，回放只依赖检查点之后的事件。
func TestReplayAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	st := svc.Store()
	setupReplayFixture(t, st, "wf")

	// 压缩检查点之前的历史（horizon=4 即检查点逻辑时间）。
	if err := st.Compact("wf", 4, RetentionPolicy{MinKeepEvents: 0}); err != nil {
		t.Fatal(err)
	}
	rs, err := st.StartReplay("wf", replayState{}, step)
	if err != nil {
		t.Fatal(err)
	}
	got := runAll(t, rs)
	want := replayState{Count: 6, Sum: 100 + 101 + 102 + 103 + 104 + 105, Last: "s"}
	if got != want {
		t.Fatalf("replay after compaction = %+v, want %+v", got, want)
	}
}

// TestReplayRequiresCheckpoint 验证无检查点时无法创建回放会话。
func TestReplayRequiresCheckpoint(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	st := svc.Store()
	commitN(t, st, "wf", 3)
	if _, err := st.StartReplay("wf", replayState{}, step); err == nil {
		t.Fatal("expected error when no checkpoint exists")
	}
}
