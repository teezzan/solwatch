package pipeline

import (
	"testing"
	"time"
)

func TestBreakerTripsOncePerWindow(t *testing.T) {
	b := &Breaker{Max: 3}
	t0 := time.Unix(1_000_000, 0)
	var trips int
	for i := 0; i < 10; i++ {
		if tripped, _ := b.Observe("hot", t0.Add(time.Duration(i)*time.Second)); tripped {
			trips++
			if i != 3 {
				t.Errorf("tripped on event %d, want the 4th (index 3)", i)
			}
		}
	}
	if trips != 1 {
		t.Errorf("tripped %d times, want 1", trips)
	}
	// A quiet address in the same period never trips.
	if tripped, _ := b.Observe("quiet", t0); tripped {
		t.Error("quiet address tripped")
	}
	// New window: counting starts over.
	if tripped, n := b.Observe("hot", t0.Add(61*time.Second)); tripped || n != 1 {
		t.Errorf("new window: tripped=%v n=%d", tripped, n)
	}
}

func TestBreakerDisabled(t *testing.T) {
	var nilB *Breaker
	if tripped, _ := nilB.Observe("x", time.Now()); tripped {
		t.Error("nil breaker tripped")
	}
	b := &Breaker{Max: 0}
	for i := 0; i < 100; i++ {
		if tripped, _ := b.Observe("x", time.Now()); tripped {
			t.Fatal("disabled breaker tripped")
		}
	}
}
