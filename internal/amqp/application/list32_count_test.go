package application

import (
	"strings"
	"testing"
)

func TestList32RejectsImplausibleCount(t *testing.T) {
	raw := []byte{
		0x00, 0x53, 0x12, 0xd0,
		0, 0, 0, 12,
		0, 0, 0, 10,
		0, 0, 0, 0, 0, 0, 0, 0,
	}
	_, _, err := DecodePerformative(raw)
	if err == nil || !strings.Contains(err.Error(), "invalid list count") {
		t.Fatalf("expected invalid list32 count error, got %v", err)
	}
}

func TestList32RejectsCountBeyondDeclaredSize(t *testing.T) {
	raw := []byte{
		0x00, 0x53, 0x12, 0xd0,
		0, 0, 0, 12,
		0, 0, 0, 9,
		0, 0, 0, 0, 0, 0, 0, 0,
	}
	_, _, err := DecodePerformative(raw)
	if err == nil || !strings.Contains(err.Error(), "invalid list count") {
		t.Fatalf("expected invalid list count error, got %v", err)
	}
}

func TestList8RejectsImplausibleCount(t *testing.T) {
	raw := []byte{
		0x00, 0x53, 0x12, 0xc0,
		5, 6,
		0, 0, 0, 0,
	}
	_, _, err := DecodePerformative(raw)
	if err == nil || !strings.Contains(err.Error(), "invalid list count") {
		t.Fatalf("expected invalid list count error, got %v", err)
	}
}

func TestList8RejectsCountBeyondDeclaredSize(t *testing.T) {
	raw := []byte{
		0x00, 0x53, 0x12, 0xc0,
		5, 7,
		0, 0, 0, 0,
	}
	_, _, err := DecodePerformative(raw)
	if err == nil || !strings.Contains(err.Error(), "invalid list count") {
		t.Fatalf("expected invalid list count error, got %v", err)
	}
}

func TestBinary8RejectsTruncatedLength(t *testing.T) {
	_, err := DecodeDataSection([]byte{0x00, 0x53, 0x75, 0xa0, 5, 1})
	if err == nil {
		t.Fatal("expected truncated binary8 error")
	}
}

func TestBinary32RejectsTruncatedLength(t *testing.T) {
	_, err := DecodeDataSection([]byte{0x00, 0x53, 0x75, 0xb0, 0, 0, 0, 5, 1})
	if err == nil {
		t.Fatal("expected truncated binary32 error")
	}
}
