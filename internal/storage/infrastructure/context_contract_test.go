package infrastructure

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func canceled() context.Context {
	c, cancel := context.WithCancel(context.Background())
	cancel()
	return c
}

func TestAppendContextDoesNotPoisonNextCall(t *testing.T) {
	l, err := OpenFileLog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, err = l.Append(canceled(), "a", []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("first append=%v", err)
	}
	if _, err = l.Append(context.Background(), "a", []byte("x")); err != nil {
		t.Fatalf("fresh append=%v", err)
	}
}

func TestReadUsesCurrentContext(t *testing.T) {
	l, err := OpenFileLog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, err = l.Append(context.Background(), "a", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err = l.Read(canceled(), 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("read=%v", err)
	}
}

func TestCompactCancellationPreservesOffsets(t *testing.T) {
	d := t.TempDir()
	l, err := OpenFileLog(d)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, err = l.Append(context.Background(), "a", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err = l.Append(context.Background(), "b", []byte("y")); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(d, "messages.log"))
	if err = l.Compact(canceled(), map[int64]struct{}{1: {}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("compact=%v", err)
	}
	after, _ := os.ReadFile(filepath.Join(d, "messages.log"))
	if string(before) != string(after) {
		t.Fatal("compact changed log")
	}
}

func TestLoadCancellationDoesNotReplaceMemory(t *testing.T) {
	d := t.TempDir()
	f := NewSnapshotFile(d)
	source := NewMemory()
	if err := f.Save(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	if err := f.Load(context.Background(), NewMemory()); err != nil {
		t.Fatal(err)
	}
	if err := f.Load(canceled(), NewMemory()); !errors.Is(err, context.Canceled) {
		t.Fatalf("load=%v", err)
	}
}

func TestSaveCancellationKeepsPreviousSnapshot(t *testing.T) {
	d := t.TempDir()
	f := NewSnapshotFile(d)
	m := NewMemory()
	if err := f.Save(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(d, "metadata.json"))
	if err := f.Save(canceled(), m); !errors.Is(err, context.Canceled) {
		t.Fatalf("save=%v", err)
	}
	after, _ := os.ReadFile(filepath.Join(d, "metadata.json"))
	if string(before) != string(after) {
		t.Fatal("save changed snapshot")
	}
}
