package mq

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type walKind string

const (
	walMsg walKind = "m"
	walOff walKind = "o"
)

type walRecord struct {
	Type      walKind `json:"t"`
	Partition int     `json:"p"`
	Offset    uint64  `json:"o"`
	ID        string  `json:"id,omitempty"`
	Key       string  `json:"k,omitempty"`
	Payload   []byte  `json:"b,omitempty"`
	Group     string  `json:"g,omitempty"`
}

// WAL is a length-prefixed JSON append log.
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

func openWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f, path: path}, nil
}

func (w *WAL) append(rec walRecord) error {
	body, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(body)))
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.f.Write(body); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *WAL) replay() ([]walRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	var out []walRecord
	for {
		var hdr [4]byte
		if _, err := io.ReadFull(w.f, hdr[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		n := binary.LittleEndian.Uint32(hdr[:])
		if n == 0 || n > 16<<20 {
			return nil, fmt.Errorf("invalid wal record size %d", n)
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(w.f, body); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		var rec walRecord
		if err := json.Unmarshal(body, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}
