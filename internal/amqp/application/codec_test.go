package application

import (
	"bytes"
	domain "github.com/enterprise/amqp-broker-core/internal/amqp/domain"
	"testing"
)

func TestPerformativeRoundTrip(t *testing.T) {
	in := domain.Performative{Descriptor: domain.DescriptorOpen, Fields: []any{"client", nil, uint32(65536), uint16(12), uint32(30000)}}
	raw, err := EncodePerformative(in)
	if err != nil {
		t.Fatal(err)
	}
	out, n, err := DecodePerformative(raw)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(raw) || out.Descriptor != domain.DescriptorOpen || out.String(0) != "client" || out.Uint(2) != 65536 {
		t.Fatalf("unexpected round trip: %#v", out)
	}
}
func TestFrameRejectsOversizeAndTruncation(t *testing.T) {
	p := domain.Performative{Descriptor: domain.DescriptorClose, Fields: []any{}}
	f, err := FrameFor(3, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := f.Marshal(1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = domain.ParseFrame(raw, 7); err == nil {
		t.Fatal("expected maximum frame rejection")
	}
	if _, err = domain.ParseFrame(raw[:len(raw)-1], 1024); err == nil {
		t.Fatal("expected truncation rejection")
	}
	parsed, err := domain.ParseFrame(raw, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Channel != 3 || !bytes.Equal(parsed.Body, f.Body) {
		t.Fatalf("unexpected parsed frame: %#v", parsed)
	}
}
func TestDataSection(t *testing.T) {
	body := []byte("payload")
	encoded := EncodeDataSection(body)
	decoded, err := DecodeDataSection(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, decoded) {
		t.Fatalf("got %q", decoded)
	}
}
