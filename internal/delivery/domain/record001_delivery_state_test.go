package domain

import (
	"testing"
	"time"
)

func newRecoveryConsumer(t *testing.T) Consumer {
	t.Helper()
	c, err := NewConsumer("consumer-a", "telemetry.raw", 2, AtLeastOnce, time.Unix(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestAcquireConsumesExactlyOneCredit(t *testing.T) {
	c := newRecoveryConsumer(t)
	if err := c.Acquire(11, "message-a"); err != nil {
		t.Fatal(err)
	}
	if c.Credit != 1 {
		t.Fatalf("credit=%d, want 1", c.Credit)
	}
}

func TestAcquireTracksCreditAndUnsettled(t *testing.T) {
	c := newRecoveryConsumer(t)
	if err := c.Acquire(12, "message-b"); err != nil {
		t.Fatal(err)
	}
	if got := c.Unsettled[12]; got != "message-b" {
		t.Fatalf("unsettled=%q, want message-b", got)
	}
}

func TestSettleClearsUnsettled(t *testing.T) {
	c := newRecoveryConsumer(t)
	c.Unsettled[13] = "message-c"
	c.Credit = 1
	if err := c.Settle(13); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Unsettled[13]; ok {
		t.Fatal("settled delivery remained in unsettled map")
	}
}

func TestReleaseReturnsExactlyOneCredit(t *testing.T) {
	c := newRecoveryConsumer(t)
	c.Unsettled[14] = "message-d"
	c.Credit = 1
	if err := c.Settle(14); err != nil {
		t.Fatal(err)
	}
	if c.Credit != 2 {
		t.Fatalf("credit=%d, want 2", c.Credit)
	}
}

func TestMessageWithoutTTLDoesNotExpire(t *testing.T) {
	m := Message{ID: "message-e", ExpiresAt: time.Time{}}
	if m.Expired(time.Unix(100, 0)) {
		t.Fatal("message without TTL reported expired")
	}
}
