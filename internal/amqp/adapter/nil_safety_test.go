package adapter

import (
	"context"
	"fmt"
	"testing"
	"time"

	amqp "github.com/enterprise/amqp-broker-core/internal/amqp/domain"
	linkapp "github.com/enterprise/amqp-broker-core/internal/link/application"
	sessionapp "github.com/enterprise/amqp-broker-core/internal/session/application"
	session "github.com/enterprise/amqp-broker-core/internal/session/domain"
)

func newNilSafetyPeer() *peer {
	registry := sessionapp.NewRegistry()
	ss := session.New("ssn", "conn", 1, 1, time.Now())
	_ = ss.Begin()
	_ = registry.Add(ss)
	return &peer{
		server:     &Server{Sessions: registry, Links: linkapp.NewRegistry()},
		sessionIDs: map[uint16]string{1: "ssn"},
		linkIDs:    map[string]string{},
	}
}

func TestFlowMissingLinkDoesNotPanic(t *testing.T) {
	p := newNilSafetyPeer()
	perf := amqp.Performative{Descriptor: amqp.DescriptorFlow, Fields: []any{uint32(0), uint32(1), false, uint32(2), uint32(7), false, uint32(3)}}
	if err := p.onFlow(context.Background(), 1, perf); err != nil {
		t.Fatalf("onFlow returned error: %v", err)
	}
}

func TestFlowMissingSessionDoesNotPanic(t *testing.T) {
	p := newNilSafetyPeer()
	p.sessionIDs[2] = "missing"
	perf := amqp.Performative{Descriptor: amqp.DescriptorFlow, Fields: []any{uint32(0), uint32(1), false, uint32(2), uint32(7), false, uint32(3)}}
	if err := p.onFlow(context.Background(), 2, perf); err == nil {
		t.Fatal("expected missing session error")
	}
}

func TestTransferMissingLinkDoesNotPanic(t *testing.T) {
	p := newNilSafetyPeer()
	p.linkIDs[fmt.Sprintf("%d:%d", 1, 7)] = "missing"
	perf := amqp.Performative{Descriptor: amqp.DescriptorTransfer, Fields: []any{uint32(7), uint32(1), false, false, true}}
	if err := p.onTransfer(context.Background(), 1, perf, nil); err == nil {
		t.Fatal("expected missing link error")
	}
}

func TestAttachMissingSessionDoesNotPanic(t *testing.T) {
	p := newNilSafetyPeer()
	p.sessionIDs[2] = "missing"
	perf := amqp.Performative{Descriptor: amqp.DescriptorAttach, Fields: []any{"link", uint32(7), false, nil, nil, "jobs"}}
	if err := p.onAttach(context.Background(), 2, perf); err == nil {
		t.Fatal("expected missing session error")
	}
}
