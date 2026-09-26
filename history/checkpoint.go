package history

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ckptFileMagic identifies a checkpoint file.
const ckptFileMagic = uint32(0x57484331) // "WHC1"

func ckptDir(dir, instance string) string {
	return filepath.Join(dir, "checkpoints", url.PathEscape(instance))
}

func ckptFileName(seq uint64) string { return fmt.Sprintf("%020d.chk", seq) }

// CreateCheckpoint durably publishes a checkpoint of the instance's
// current committed state and returns it. The checkpoint is written
// atomically (tmp + rename) so a crash never leaves a half-written
// checkpoint behind.
func (s *Service) CreateCheckpoint(instanceID string) (Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Checkpoint{}, ErrClosed
	}
	inst, ok := s.getInstance(instanceID)
	if !ok {
		return Checkpoint{}, ErrNotFound
	}
	v := inst.view.Load()
	if v.lastSeq == 0 && inst.checkpoint.Load() == nil {
		return Checkpoint{}, ErrNotFound // no committed state yet
	}
	c := Checkpoint{
		InstanceID:  instanceID,
		Seq:         v.lastSeq,
		LogicalTime: v.lastTime,
		State:       v.state,
		Vars:        cloneVars(v.vars),
		WallTime:    time.Now().UTC(),
	}
	if err := writeCheckpointFile(s.dir, c, s.opts.NoFsync); err != nil {
		return Checkpoint{}, err
	}
	inst.checkpoint.Store(&c)
	if err := pruneCheckpoints(s.dir, instanceID, s.opts.KeepCheckpoints); err != nil {
		return Checkpoint{}, err
	}
	if s.policy.AutoMaintain {
		if err := s.maintainLocked(instanceID); err != nil {
			return Checkpoint{}, err
		}
	}
	return c, nil
}

// LatestCheckpoint returns the most recent committed checkpoint.
func (s *Service) LatestCheckpoint(instanceID string) (Checkpoint, bool, error) {
	inst, ok := s.getInstance(instanceID)
	if !ok {
		return Checkpoint{}, false, ErrNotFound
	}
	if c := inst.checkpoint.Load(); c != nil {
		return *c, true, nil
	}
	return Checkpoint{}, false, nil
}

// writeCheckpointFile atomically persists a checkpoint.
func writeCheckpointFile(dir string, c Checkpoint, noFsync bool) error {
	cd := ckptDir(dir, c.InstanceID)
	if err := os.MkdirAll(cd, 0o755); err != nil {
		return err
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return err
	}
	buf := make([]byte, 12+len(payload))
	binary.LittleEndian.PutUint32(buf[0:], ckptFileMagic)
	binary.LittleEndian.PutUint32(buf[4:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[8:], crc32.ChecksumIEEE(payload))
	copy(buf[12:], payload)
	name := ckptFileName(c.Seq)
	tmp := filepath.Join(cd, name+".tmp")
	return writeFileAtomic(tmp, filepath.Join(cd, name), buf, noFsync)
}

func readCheckpointFile(path string) (Checkpoint, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Checkpoint{}, err
	}
	if len(raw) < 12 || binary.LittleEndian.Uint32(raw[0:]) != ckptFileMagic {
		return Checkpoint{}, fmt.Errorf("history: bad checkpoint file %s", path)
	}
	plen := binary.LittleEndian.Uint32(raw[4:])
	crc := binary.LittleEndian.Uint32(raw[8:])
	if int(plen) != len(raw)-12 {
		return Checkpoint{}, fmt.Errorf("history: truncated checkpoint file %s", path)
	}
	payload := raw[12:]
	if crc32.ChecksumIEEE(payload) != crc {
		return Checkpoint{}, fmt.Errorf("history: corrupt checkpoint file %s", path)
	}
	var c Checkpoint
	if err := json.Unmarshal(payload, &c); err != nil {
		return Checkpoint{}, fmt.Errorf("history: decode checkpoint %s: %w", path, err)
	}
	return c, nil
}

func listCheckpointFiles(dir, instance string) ([]string, error) {
	entries, err := os.ReadDir(ckptDir(dir, instance))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".chk" {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out) // zero-padded seq sorts lexicographically
	return out, nil
}

func loadLatestCheckpoint(dir, instance string) (Checkpoint, bool, error) {
	files, err := listCheckpointFiles(dir, instance)
	if err != nil || len(files) == 0 {
		return Checkpoint{}, false, err
	}
	c, err := readCheckpointFile(filepath.Join(ckptDir(dir, instance), files[len(files)-1]))
	if err != nil {
		return Checkpoint{}, false, err
	}
	return c, true, nil
}

func loadCheckpointBySeq(dir, instance string, seq uint64) (Checkpoint, bool, error) {
	c, err := readCheckpointFile(filepath.Join(ckptDir(dir, instance), ckptFileName(seq)))
	if os.IsNotExist(err) {
		return Checkpoint{}, false, nil
	}
	if err != nil {
		return Checkpoint{}, false, err
	}
	return c, true, nil
}

// pruneCheckpoints keeps the newest keep checkpoint files per instance.
func pruneCheckpoints(dir, instance string, keep int) error {
	if keep <= 0 {
		keep = 1
	}
	files, err := listCheckpointFiles(dir, instance)
	if err != nil {
		return err
	}
	for len(files) > keep {
		if err := os.Remove(filepath.Join(ckptDir(dir, instance), files[0])); err != nil {
			return err
		}
		files = files[1:]
	}
	return nil
}
