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

// segment is an immutable (once sealed) ordered run of events of one
// instance, sorted by (LogicalTime, Seq).
type segment struct {
	events []Event
}

func (s *segment) firstSeq() uint64 {
	if len(s.events) == 0 {
		return 0
	}
	return s.events[0].Seq
}

func (s *segment) lastSeq() uint64 {
	if len(s.events) == 0 {
		return 0
	}
	return s.events[len(s.events)-1].Seq
}

func (s *segment) minTime() int64 {
	if len(s.events) == 0 {
		return 0
	}
	return s.events[0].LogicalTime
}

func (s *segment) maxTime() int64 {
	if len(s.events) == 0 {
		return 0
	}
	return s.events[len(s.events)-1].LogicalTime
}

// segFileMagic identifies a segment file.
const segFileMagic = uint32(0x57485331) // "WHS1"

// segManifest lists the live segment files of one instance, in seq order.
// It is the source of truth: segment files not referenced by the manifest
// are orphans and removed during recovery.
type segManifest struct {
	Instance string   `json:"instance"`
	Files    []string `json:"files"`
}

// segDir returns the directory holding one instance's segments.
func segDir(dir, instance string) string {
	return filepath.Join(dir, "segments", url.PathEscape(instance))
}

// segFilePayload is the JSON body of a segment file.
type segFilePayload struct {
	Instance string  `json:"instance"`
	Events   []Event `json:"events"`
}

// writeSegmentFile atomically writes a sealed segment and returns its
// base file name. Format: magic(4) | len(4) | crc32(4) | JSON payload.
func writeSegmentFile(dir, instance string, events []Event, noFsync bool) (string, error) {
	if len(events) == 0 {
		return "", fmt.Errorf("history: refusing to write empty segment")
	}
	sd := segDir(dir, instance)
	if err := os.MkdirAll(sd, 0o755); err != nil {
		return "", err
	}
	name := fmt.Sprintf("%020d-%020d-%d.seg", events[0].Seq, events[len(events)-1].Seq, time.Now().UnixNano())
	payload, err := json.Marshal(segFilePayload{Instance: instance, Events: events})
	if err != nil {
		return "", err
	}
	buf := make([]byte, 12+len(payload))
	binary.LittleEndian.PutUint32(buf[0:], segFileMagic)
	binary.LittleEndian.PutUint32(buf[4:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[8:], crc32.ChecksumIEEE(payload))
	copy(buf[12:], payload)

	tmp := filepath.Join(sd, name+".tmp")
	if err := writeFileAtomic(tmp, filepath.Join(sd, name), buf, noFsync); err != nil {
		return "", err
	}
	return name, nil
}

// readSegmentFile loads and validates a segment file.
func readSegmentFile(path string) ([]Event, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) < 12 || binary.LittleEndian.Uint32(raw[0:]) != segFileMagic {
		return nil, fmt.Errorf("history: bad segment file %s", path)
	}
	plen := binary.LittleEndian.Uint32(raw[4:])
	crc := binary.LittleEndian.Uint32(raw[8:])
	if int(plen) != len(raw)-12 {
		return nil, fmt.Errorf("history: truncated segment file %s", path)
	}
	payload := raw[12:]
	if crc32.ChecksumIEEE(payload) != crc {
		return nil, fmt.Errorf("history: corrupt segment file %s", path)
	}
	var p segFilePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("history: decode segment %s: %w", path, err)
	}
	return p.Events, nil
}

func segManifestPath(dir, instance string) string {
	return filepath.Join(segDir(dir, instance), "manifest.json")
}

func loadSegManifest(dir, instance string) (segManifest, bool, error) {
	raw, err := os.ReadFile(segManifestPath(dir, instance))
	if os.IsNotExist(err) {
		return segManifest{}, false, nil
	}
	if err != nil {
		return segManifest{}, false, err
	}
	var m segManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return segManifest{}, false, fmt.Errorf("history: decode manifest: %w", err)
	}
	return m, true, nil
}

// writeSegManifest atomically publishes a manifest (tmp + rename).
func writeSegManifest(dir string, m segManifest, noFsync bool) error {
	sd := segDir(dir, m.Instance)
	if err := os.MkdirAll(sd, 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(sd, "manifest.json.tmp")
	return writeFileAtomic(tmp, filepath.Join(sd, "manifest.json"), raw, noFsync)
}

// listSegmentFiles returns the base names of all *.seg files of an
// instance, sorted.
func listSegmentFiles(dir, instance string) ([]string, error) {
	entries, err := os.ReadDir(segDir(dir, instance))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".seg" {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

// writeFileAtomic writes buf to tmp, fsyncs, renames to final and fsyncs
// the directory.
func writeFileAtomic(tmp, final string, buf []byte, noFsync bool) error {
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if !noFsync {
		if err := f.Sync(); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	if !noFsync {
		fsyncDir(filepath.Dir(final))
	}
	return nil
}

// removeFile removes one file from a directory, ignoring missing files.
func removeFile(dir, name string) error {
	return os.Remove(filepath.Join(dir, name))
}
