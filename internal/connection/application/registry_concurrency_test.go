package application

import (
	domain "github.com/enterprise/amqp-broker-core/internal/connection/domain"
	"sync"
	"testing"
	"time"
)

func TestConcurrentRegistryAdmissionRespectsLimit(t *testing.T) {
	for round := 0; round < 5; round++ {
		r := NewRegistry(8)
		start := make(chan struct{})
		var wg sync.WaitGroup
		var mu sync.Mutex
		ok := 0
		for i := 0; i < 128; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				if r.Add(domain.New(string(rune(i+1)), "r", 1, time.Minute, time.Now())) == nil {
					mu.Lock()
					ok++
					mu.Unlock()
				}
			}(i)
		}
		close(start)
		wg.Wait()
		if ok != 8 || r.Count() != 8 {
			t.Fatalf("round %d successes=%d count=%d", round, ok, r.Count())
		}
	}
}

func TestExpiredScanDuringTouchHasNoRace(t *testing.T) {
	r := NewRegistry(2)
	c := domain.New("c", "r", 1, time.Hour, time.Now())
	_ = r.Add(c)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 10000; i++ {
			c.Touch(time.Now())
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 10000; i++ {
			_ = r.Expired(time.Now())
		}
	}()
	close(start)
	wg.Wait()
}
