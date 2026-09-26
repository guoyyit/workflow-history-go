package history

import (
	"os"
	"path/filepath"
	"testing"
)

// TestRecoverAfterReopen 验证正常关闭重开后，已确认历史与检查点完整可见，
// 且新提交的全局序号从恢复值继续递增、不复用。
func TestRecoverAfterReopen(t *testing.T) {
	dir := t.TempDir()

	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	st := svc.Store()
	for i := 0; i < 3; i++ {
		if _, err := st.Commit("a", Transition{To: "s", Vars: map[string]any{"i": i}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := st.Commit("b", Transition{To: "s", Vars: map[string]any{"i": i}}); err != nil {
			t.Fatal(err)
		}
	}
	cp, err := st.SaveCheckpoint("a")
	if err != nil {
		t.Fatal(err)
	}
	lastEv, err := st.Commit("a", Transition{To: "s", Vars: map[string]any{"i": 3}})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}

	// 重开恢复。
	svc2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc2.Close()
	st2 := svc2.Store()

	snap, err := st2.QueryAt("a", 4)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Vars["i"] != 3.0 { // JSON 恢复后数字为 float64
		t.Fatalf("recovered snapshot: %+v", snap)
	}
	snap, err = st2.QueryAt("a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Vars["i"] != 1.0 {
		t.Fatalf("time-travel after recovery: %+v", snap)
	}
	gotCP, err := st2.LatestCheckpoint("a")
	if err != nil {
		t.Fatal(err)
	}
	if gotCP.LogicalTime != cp.LogicalTime || gotCP.Seq != cp.Seq {
		t.Fatalf("checkpoint not recovered: %+v vs %+v", gotCP, cp)
	}
	// 序号不复用：新提交必须大于恢复前最大序号。
	ev, err := st2.Commit("a", Transition{To: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if ev.Seq <= lastEv.Seq {
		t.Fatalf("seq reused after recovery: %d <= %d", ev.Seq, lastEv.Seq)
	}
	if ev.LogicalTime != 5 {
		t.Fatalf("logical clock not recovered: %d, want 5", ev.LogicalTime)
	}
}

// simulateCrash 模拟崩溃：不关闭旧服务，直接向 WAL 尾部写入 data，
// 然后重新打开存储（openWAL 会截断不完整记录）。
func simulateCrash(t *testing.T, dir string, data []byte) *Service {
	t.Helper()
	walPath := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	f.Sync()
	f.Close()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

// TestCrashTruncatesTornTail 验证崩溃遗留的半截写入被截断，
// 已确认历史完整可见，且截断后新提交正常工作。
func TestCrashTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	st := svc.Store()
	for i := 0; i < 3; i++ {
		if _, err := st.Commit("wf", Transition{To: "s", Vars: map[string]any{"i": i}}); err != nil {
			t.Fatal(err)
		}
	}
	// 崩溃：WAL 尾部混入半截记录与随机垃圾（旧服务不再使用）。
	torn := []byte{0x57, 0x48, 0x4c, 0x47, 0x01, 0x00, 0x00}
	svc2 := simulateCrash(t, dir, append(torn, []byte("garbage-tail")...))
	defer svc2.Close()
	st2 := svc2.Store()

	snap, err := st2.QueryAt("wf", 3)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Vars["i"] != 2.0 {
		t.Fatalf("committed history lost: %+v", snap)
	}
	// 查询更晚的时间点：只能看到第 3 次提交，半截写入不可见。
	snap, err = st2.QueryAt("wf", 100)
	if err != nil {
		t.Fatal(err)
	}
	if snap.LogicalTime != 3 || snap.Vars["i"] != 2.0 {
		t.Fatalf("torn write became visible: %+v", snap)
	}
	// 截断后新提交正常，且重开后依然只有确认的历史。
	if _, err := st2.Commit("wf", Transition{To: "s", Vars: map[string]any{"i": 3}}); err != nil {
		t.Fatal(err)
	}
	svc2.Close()
	svc3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc3.Close()
	snap, err = svc3.Store().QueryAt("wf", 4)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Vars["i"] != 3.0 {
		t.Fatalf("post-crash commit not durable: %+v", snap)
	}
}

// TestUncommittedRecordNotVisible 验证完整编码但未被服务确认的
// 带外写入不会出现在已确认历史中（模拟崩溃发生在应用视图之前）。
func TestUncommittedRecordNotVisible(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	st := svc.Store()
	if _, err := st.Commit("wf", Transition{To: "s1", Vars: map[string]any{"i": 1}}); err != nil {
		t.Fatal(err)
	}
	// 构造一条"未来"事件记录，但只写入一半（崩溃于写入中途）。
	half := record{typ: recEvent, ev: &Event{
		InstanceID: "wf", Seq: 99, LogicalTime: 2, To: "s2",
		Vars: map[string]any{"i": 2},
	}}
	buf, err := encodeRecord(half)
	if err != nil {
		t.Fatal(err)
	}
	svc2 := simulateCrash(t, dir, buf[:len(buf)/2])
	defer svc2.Close()
	// 查询逻辑时间 2：只能命中已确认的第 1 次提交，未确认记录不可见。
	snap, err := svc2.Store().QueryAt("wf", 2)
	if err != nil {
		t.Fatal(err)
	}
	if snap.LogicalTime != 1 || snap.State != "s1" {
		t.Fatalf("uncommitted record visible: %+v", snap)
	}
}
