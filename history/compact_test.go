package history

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// commitN 提交 n 次迁移，变量 i 从 0 到 n-1。
func commitN(t *testing.T, st *Store, id string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := st.Commit(id, Transition{To: "s", Vars: map[string]any{"i": i}}); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCompactMergesIntoBase 验证压缩将 horizon 之前的事件合并为基线，
// 且 horizon 及之后的时间点查询结果不变，之前的查询命中基线。
func TestCompactMergesIntoBase(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	st := svc.Store()

	commitN(t, st, "wf", 10) // 逻辑时间 1..10，变量 i=0..9
	if _, err := st.SaveCheckpoint("wf"); err != nil {
		t.Fatal(err)
	}
	// 压缩 horizon=5，不限制最少保留条数。
	if err := st.Compact("wf", 5, RetentionPolicy{MinKeepEvents: 0}); err != nil {
		t.Fatal(err)
	}

	// horizon 及之后：查询结果逐字节不变。
	for lt := 5; lt <= 10; lt++ {
		snap, err := st.QueryAt("wf", uint64(lt))
		if err != nil {
			t.Fatal(err)
		}
		if !snap.Exact || snap.Vars["i"] != lt-1 {
			t.Fatalf("t=%d changed after compaction: %+v", lt, snap)
		}
	}
	// horizon 之前：命中基线（逻辑时间 5 的状态），Exact=false。
	snap, err := st.QueryAt("wf", 3)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Exact || snap.LogicalTime != 5 || snap.Vars["i"] != 4 {
		t.Fatalf("expected base snapshot for t=3: %+v", snap)
	}
}

// TestCompactGuards 验证压缩的安全约束：
//   - 无检查点禁止压缩；
//   - horizon 超过最近检查点被拒绝；
//   - MinKeepEvents 条最近事件始终保留。
func TestCompactGuards(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	st := svc.Store()

	commitN(t, st, "wf", 6)
	// 无检查点：禁止压缩。
	if err := st.Compact("wf", 3, RetentionPolicy{}); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("err=%v, want ErrNoCheckpoint", err)
	}
	// 在逻辑时间 4 处建立检查点，之后又提交 2 条未覆盖事件。
	// （先重建一个实例使检查点不在末尾。）
	commitN(t, st, "wf2", 4)
	if _, err := st.SaveCheckpoint("wf2"); err != nil {
		t.Fatal(err)
	}
	// 逻辑时间 5、6，变量 i 分别为 104、105，便于与前面批次区分。
	for _, i := range []int{104, 105} {
		if _, err := st.Commit("wf2", Transition{To: "s", Vars: map[string]any{"i": i}}); err != nil {
			t.Fatal(err)
		}
	}
	// horizon 超过检查点：拒绝。
	if err := st.Compact("wf2", 5, RetentionPolicy{}); !errors.Is(err, ErrHorizonUncovered) {
		t.Fatalf("err=%v, want ErrHorizonUncovered", err)
	}
	// horizon=4 且 MinKeepEvents=2：最后 2 条保留，未被覆盖的事件绝不丢失。
	if err := st.Compact("wf2", 4, RetentionPolicy{MinKeepEvents: 2}); err != nil {
		t.Fatal(err)
	}
	for lt, want := range map[uint64]int{5: 104, 6: 105} {
		snap, err := st.QueryAt("wf2", lt)
		if err != nil {
			t.Fatalf("uncovered event lost: t=%d err=%v", lt, err)
		}
		if !snap.Exact || snap.Vars["i"] != want {
			t.Fatalf("uncovered event wrong: %+v, want i=%d", snap, want)
		}
	}
	// 基线应建立在逻辑时间 4（保留最后 2 条：5、6；丢弃 1..4）。
	snap, err := st.QueryAt("wf2", 2)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Exact || snap.LogicalTime != 4 {
		t.Fatalf("base mismatch: %+v", snap)
	}
}

// TestCompactPersistsAcrossReopen 验证压缩结果经 WAL 重写后持久化，
// 重开后时间点查询语义不变，且 WAL 文件确实变小。
func TestCompactPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	st := svc.Store()
	commitN(t, st, "wf", 20)
	if _, err := st.SaveCheckpoint("wf"); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(dir, walFileName)
	fiBefore, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Compact("wf", 15, RetentionPolicy{MinKeepEvents: 0}); err != nil {
		t.Fatal(err)
	}
	fiAfter, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if fiAfter.Size() >= fiBefore.Size() {
		t.Fatalf("WAL not shrunk: before=%d after=%d", fiBefore.Size(), fiAfter.Size())
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}

	svc2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc2.Close()
	st2 := svc2.Store()
	// horizon 之后精确可查。
	snap, err := st2.QueryAt("wf", 18)
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Exact || snap.Vars["i"] != 17.0 {
		t.Fatalf("post-reopen query: %+v", snap)
	}
	// horizon 之前命中基线。
	snap, err = st2.QueryAt("wf", 7)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Exact || snap.LogicalTime != 15 {
		t.Fatalf("post-reopen base: %+v", snap)
	}
	// 压缩后新提交的事件正常追加。
	if _, err := st2.Commit("wf", Transition{To: "s", Vars: map[string]any{"i": 20}}); err != nil {
		t.Fatal(err)
	}
	snap, err = st2.QueryAt("wf", 21)
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Exact || snap.Vars["i"] != 20 {
		t.Fatalf("post-compaction commit: %+v", snap)
	}
}

// TestCompactDoesNotAffectOtherInstances 验证压缩只影响目标实例。
func TestCompactDoesNotAffectOtherInstances(t *testing.T) {
	dir := t.TempDir()
	svc, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	st := svc.Store()

	commitN(t, st, "x", 8)
	commitN(t, st, "y", 8)
	if _, err := st.SaveCheckpoint("x"); err != nil {
		t.Fatal(err)
	}
	if err := st.Compact("x", 6, RetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	// y 的全部历史仍精确可查。
	for lt := 1; lt <= 8; lt++ {
		snap, err := st.QueryAt("y", uint64(lt))
		if err != nil {
			t.Fatal(err)
		}
		if !snap.Exact || snap.Vars["i"] != lt-1 {
			t.Fatalf("instance y affected: t=%d %+v", lt, snap)
		}
	}
}
