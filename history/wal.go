package history

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// recordType 标识 WAL 记录类型。
type recordType uint8

const (
	recEvent      recordType = 1 // 已提交事件
	recCheckpoint recordType = 2 // 已提交检查点
	recBase       recordType = 3 // 压缩产生的基线快照
)

// record 是 WAL 中的一条记录（解码后的内存形态）。
type record struct {
	typ  recordType
	ev   *Event
	cp   *Checkpoint
	base *Checkpoint // recBase 使用，语义同 Checkpoint
}

// WAL 记录物理格式：
//
//	[magic uint32][typ uint8][payloadLen uint32][crc32 uint32][payload...]
//
// crc32 覆盖 typ||payloadLen||payload。恢复时遇到魔数错误、长度越界、
// 截断或校验失败的记录即视为崩溃遗留的未完成写入，停止并截断。
const (
	walMagic   uint32 = 0x57484C47 // "WHLG"
	headerSize        = 4 + 1 + 4 + 4
)

// wal 是追加式预写日志，负责持久化与崩溃恢复。
type wal struct {
	f      *os.File
	path   string
	offset int64 // 下一条记录的写入位置（即已确认内容的末尾）
}

// encodeRecord 将记录序列化为物理字节。
func encodeRecord(r record) ([]byte, error) {
	var payload []byte
	var err error
	switch r.typ {
	case recEvent:
		payload, err = json.Marshal(r.ev)
	case recCheckpoint, recBase:
		if r.typ == recCheckpoint {
			payload, err = json.Marshal(r.cp)
		} else {
			payload, err = json.Marshal(r.base)
		}
	default:
		return nil, fmt.Errorf("history: unknown record type %d", r.typ)
	}
	if err != nil {
		return nil, err
	}
	buf := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], walMagic)
	buf[4] = byte(r.typ)
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(payload)))
	crc := crc32.NewIEEE()
	crc.Write(buf[4:9])
	crc.Write(payload)
	binary.BigEndian.PutUint32(buf[9:13], crc.Sum32())
	copy(buf[headerSize:], payload)
	return buf, nil
}

// decodeRecord 从 b 的头部解析一条记录，返回记录与消耗的字节数。
// 数据不完整或损坏时返回 io.ErrUnexpectedEOF。
func decodeRecord(b []byte) (record, int, error) {
	if len(b) < headerSize {
		return record{}, 0, io.ErrUnexpectedEOF
	}
	if binary.BigEndian.Uint32(b[0:4]) != walMagic {
		return record{}, 0, io.ErrUnexpectedEOF
	}
	typ := recordType(b[4])
	payloadLen := int(binary.BigEndian.Uint32(b[5:9]))
	wantCRC := binary.BigEndian.Uint32(b[9:13])
	if payloadLen < 0 || len(b) < headerSize+payloadLen {
		return record{}, 0, io.ErrUnexpectedEOF
	}
	payload := b[headerSize : headerSize+payloadLen]
	crc := crc32.NewIEEE()
	crc.Write(b[4:9])
	crc.Write(payload)
	if crc.Sum32() != wantCRC {
		return record{}, 0, io.ErrUnexpectedEOF
	}
	r := record{typ: typ}
	switch typ {
	case recEvent:
		var ev Event
		if err := json.Unmarshal(payload, &ev); err != nil {
			return record{}, 0, io.ErrUnexpectedEOF
		}
		r.ev = &ev
	case recCheckpoint, recBase:
		var cp Checkpoint
		if err := json.Unmarshal(payload, &cp); err != nil {
			return record{}, 0, io.ErrUnexpectedEOF
		}
		if typ == recCheckpoint {
			r.cp = &cp
		} else {
			r.base = &cp
		}
	default:
		return record{}, 0, io.ErrUnexpectedEOF
	}
	return r, headerSize + payloadLen, nil
}

// scanValid 返回 data 中从开头起连续完整记录的总字节数。
func scanValid(data []byte) int {
	off := 0
	for {
		_, n, err := decodeRecord(data[off:])
		if err != nil {
			return off
		}
		off += n
	}
}

// openWAL 打开（或创建）WAL 文件，并将尾部未完成的写入截断。
func openWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	valid := scanValid(data)
	if valid < len(data) {
		// 截断崩溃遗留的未完成写入。
		if err := f.Truncate(int64(valid)); err != nil {
			f.Close()
			return nil, err
		}
	}
	if _, err := f.Seek(int64(valid), io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return &wal{f: f, path: path, offset: int64(valid)}, nil
}

// append 写入一条记录并 fsync，返回后记录即已确认持久化。
func (w *wal) append(r record) error {
	buf, err := encodeRecord(r)
	if err != nil {
		return err
	}
	if _, err := w.f.WriteAt(buf, w.offset); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.offset += int64(len(buf))
	return nil
}

// readAll 从头顺序读取所有完整记录，遇到损坏或不完整的尾部即停止。
func readAll(path string) ([]record, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []record
	off := 0
	for off < len(data) {
		r, n, err := decodeRecord(data[off:])
		if err != nil {
			break
		}
		out = append(out, r)
		off += n
	}
	return out, nil
}

// rewrite 用给定记录集合整体替换 WAL：先写临时文件并 fsync，
// 再原子 rename，保证压缩过程崩溃安全。
func (w *wal) rewrite(recs []record) error {
	tmp := w.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	var off int64
	for _, r := range recs {
		buf, err := encodeRecord(r)
		if err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		if _, err := f.WriteAt(buf, off); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		off += int64(len(buf))
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, w.path); err != nil {
		return err
	}
	// fsync 目录，确保 rename 持久化。
	if d, err := os.Open(dirOf(w.path)); err == nil {
		d.Sync()
		d.Close()
	}
	// 重新打开文件句柄指向新文件。
	nf, err := os.OpenFile(w.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	w.f.Close()
	w.f = nf
	w.offset = off
	return nil
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			if i == 0 {
				return "/"
			}
			return path[:i]
		}
	}
	return "."
}

// close 关闭 WAL 文件。
func (w *wal) close() error {
	return w.f.Close()
}
