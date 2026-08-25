package infrastructure

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type FileLog struct {
	mu      sync.Mutex
	path    string
	file    *os.File
	offsets map[int64]int64
}

func OpenFileLog(dir string) (*FileLog, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	path := filepath.Join(dir, "messages.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open message log: %w", err)
	}
	l := &FileLog{path: path, file: f, offsets: map[int64]int64{}}
	if err = l.reindex(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}
func (l *FileLog) reindex() error {
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var logical int64
	for {
		pos, err := l.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		var n uint32
		if err = binary.Read(l.file, binary.BigEndian, &n); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("read log header: %w", err)
		}
		if n > 64<<20 {
			return errors.New("corrupt message log record")
		}
		l.offsets[logical] = pos
		if _, err = l.file.Seek(int64(n), io.SeekCurrent); err != nil {
			return err
		}
		logical++
	}
	_, err := l.file.Seek(0, io.SeekEnd)
	return err
}
func (l *FileLog) Append(ctx context.Context, id string, body []byte) (int64, error) {
	if len(body) > 64<<20 {
		return 0, errors.New("message body exceeds log limit")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	pos, err := l.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	record := append([]byte(id+"\n"), body...)
	if err = binary.Write(l.file, binary.BigEndian, uint32(len(record))); err != nil {
		return 0, err
	}
	if _, err = l.file.Write(record); err != nil {
		return 0, err
	}
	if err = l.file.Sync(); err != nil {
		return 0, err
	}
	logical := int64(len(l.offsets))
	l.offsets[logical] = pos
	return logical, nil
}
func (l *FileLog) Read(ctx context.Context, offset int64) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pos, ok := l.offsets[offset]
	if !ok {
		return nil, errors.New("log offset not found")
	}
	if _, err := l.file.Seek(pos, io.SeekStart); err != nil {
		return nil, err
	}
	var n uint32
	if err := binary.Read(l.file, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(l.file, data); err != nil {
		return nil, err
	}
	scan := bufio.NewScanner(bytesReader(data))
	if !scan.Scan() {
		return nil, errors.New("invalid log record")
	}
	cut := len(scan.Bytes()) + 1
	if cut > len(data) {
		return nil, errors.New("invalid log delimiter")
	}
	return append([]byte(nil), data[cut:]...), nil
}
func (l *FileLog) Compact(ctx context.Context, live map[int64]struct{}) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(l.path), "compact-*.log")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	newOffsets := map[int64]int64{}
	var next int64
	for old := int64(0); old < int64(len(l.offsets)); old++ {
		if _, ok := live[old]; !ok {
			continue
		}
		pos := l.offsets[old]
		if _, err = l.file.Seek(pos, io.SeekStart); err != nil {
			return err
		}
		var n uint32
		if err = binary.Read(l.file, binary.BigEndian, &n); err != nil {
			return err
		}
		data := make([]byte, n)
		if _, err = io.ReadFull(l.file, data); err != nil {
			return err
		}
		newPos, _ := tmp.Seek(0, io.SeekCurrent)
		if err = binary.Write(tmp, binary.BigEndian, n); err != nil {
			return err
		}
		if _, err = tmp.Write(data); err != nil {
			return err
		}
		newOffsets[next] = newPos
		next++
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = l.file.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp.Name(), l.path); err != nil {
		return err
	}
	l.file, err = os.OpenFile(l.path, os.O_RDWR, 0600)
	l.offsets = newOffsets
	return err
}
func (l *FileLog) Close() error { l.mu.Lock(); defer l.mu.Unlock(); return l.file.Close() }

type sliceReader struct{ b []byte }

func bytesReader(b []byte) *sliceReader { return &sliceReader{b: b} }
func (r *sliceReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}
