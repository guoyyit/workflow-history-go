package history

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// walMagic identifies a WAL record.
const walMagic = uint32(0x57484C31) // "WHL1"

// walHeaderLen is magic(4) + payloadLen(4) + crc32(4).
const walHeaderLen = 12

// wal is an append-only write-ahead log of committed event batches.
// Each record: magic(4) | payloadLen uint32 LE | crc32(payload) uint32 LE |
// payload(JSON []Event). A batch is atomic: on recovery it is either fully
// valid or ignored, so an unfinished write never becomes confirmed state.
type wal struct {
	f       *os.File
	path    string
	noFsync bool
}

func walPath(dir string) string { return filepath.Join(dir, "wal.log") }

// openWAL opens (creating if needed) the WAL in dir, returns the valid
// committed batches found, and truncates any torn tail so later appends
// never follow garbage.
func openWAL(dir string, noFsync bool) (*wal, [][]Event, error) {
	path := walPath(dir)
	batches, validLen, err := readWALBatches(path)
	if err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("history: open wal: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if st.Size() != validLen {
		// Torn or corrupt tail: truncate to the last valid record.
		if err := f.Truncate(validLen); err != nil {
			f.Close()
			return nil, nil, fmt.Errorf("history: truncate wal: %w", err)
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, nil, err
	}
	return &wal{f: f, path: path, noFsync: noFsync}, batches, nil
}

// readWALBatches parses all valid records in path and reports the offset
// just past the last valid record. A missing file is not an error.
func readWALBatches(path string) ([][]Event, int64, error) {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("history: read wal: %w", err)
	}
	var batches [][]Event
	var off int64
	for off+walHeaderLen <= int64(len(raw)) {
		magic := binary.LittleEndian.Uint32(raw[off:])
		if magic != walMagic {
			break
		}
		plen := binary.LittleEndian.Uint32(raw[off+4:])
		crc := binary.LittleEndian.Uint32(raw[off+8:])
		end := off + walHeaderLen + int64(plen)
		if plen < 0 || end > int64(len(raw)) {
			break // torn tail
		}
		payload := raw[off+walHeaderLen : end]
		if crc32.ChecksumIEEE(payload) != crc {
			break // corrupt record
		}
		var events []Event
		if err := json.Unmarshal(payload, &events); err != nil {
			break // undecodable record: treat as end of log
		}
		batches = append(batches, events)
		off = end
	}
	return batches, off, nil
}

func encodeWALRecord(events []Event) ([]byte, error) {
	payload, err := json.Marshal(events)
	if err != nil {
		return nil, err
	}
	rec := make([]byte, walHeaderLen+len(payload))
	binary.LittleEndian.PutUint32(rec[0:], walMagic)
	binary.LittleEndian.PutUint32(rec[4:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(rec[8:], crc32.ChecksumIEEE(payload))
	copy(rec[walHeaderLen:], payload)
	return rec, nil
}

// appendBatch durably appends one committed batch.
func (w *wal) appendBatch(events []Event) error {
	rec, err := encodeWALRecord(events)
	if err != nil {
		return err
	}
	if _, err := w.f.Write(rec); err != nil {
		return fmt.Errorf("history: wal append: %w", err)
	}
	if !w.noFsync {
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("history: wal sync: %w", err)
		}
	}
	return nil
}

// rewrite atomically replaces the WAL content with the given batches:
// write wal.log.tmp, fsync, rename over wal.log, fsync the directory.
// A crash before the rename leaves the old WAL intact; a crash after
// leaves the new one. A leftover .tmp is ignored on open.
func (w *wal) rewrite(batches [][]Event) error {
	tmp := w.path + ".tmp"
	tf, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("history: wal rewrite open: %w", err)
	}
	for _, b := range batches {
		rec, err := encodeWALRecord(b)
		if err != nil {
			tf.Close()
			os.Remove(tmp)
			return err
		}
		if _, err := tf.Write(rec); err != nil {
			tf.Close()
			os.Remove(tmp)
			return fmt.Errorf("history: wal rewrite write: %w", err)
		}
	}
	if !w.noFsync {
		if err := tf.Sync(); err != nil {
			tf.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := tf.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, w.path); err != nil {
		return fmt.Errorf("history: wal rename: %w", err)
	}
	if !w.noFsync {
		fsyncDir(filepath.Dir(w.path))
	}
	// Swap the file handle to the new file.
	nf, err := os.OpenFile(w.path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("history: wal reopen: %w", err)
	}
	w.f.Close()
	w.f = nf
	return nil
}

func (w *wal) close() error {
	if w.f == nil {
		return nil
	}
	if !w.noFsync {
		w.f.Sync()
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// fsyncDir flushes directory metadata (best effort).
func fsyncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	defer d.Close()
	d.Sync()
}
