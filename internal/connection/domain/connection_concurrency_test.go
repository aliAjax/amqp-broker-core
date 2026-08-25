package domain

import (
	"sync"
	"testing"
	"time"
)

func TestConcurrentSessionAttachIsAtomic(t *testing.T) {
	c := New("c", "r", 1, time.Minute, time.Now())
	_ = c.Transition(StateHeader)
	_ = c.Transition(StateOpen)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if c.AddSession(1, string(rune(i+1))) == nil {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if ok != 1 || c.Snapshot().SessionCount != 1 {
		t.Fatalf("success=%d snapshot=%d", ok, c.Snapshot().SessionCount)
	}
}

func TestSnapshotDuringSessionChurnHasNoRace(t *testing.T) {
	c := New("c", "r", 1, time.Minute, time.Now())
	_ = c.Transition(StateHeader)
	_ = c.Transition(StateOpen)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 10000; i++ {
			c.Touch(time.Now())
			_ = c.AddSession(uint16(i%32), "x")
			c.RemoveSession(uint16(i % 32))
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 10000; i++ {
			s := c.Snapshot()
			if s.ID != "c" {
				t.Errorf("bad id")
			}
		}
	}()
	close(start)
	wg.Wait()
}
