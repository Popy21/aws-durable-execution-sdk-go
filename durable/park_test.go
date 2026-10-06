package durable

import (
	"testing"
	"time"
)

// TestParkerSignalBeforeParkDoesNotSuspend delays the waiting goroutine
// between a worker's signal and its own park, then keeps it from receiving
// for several settle periods. A pending signal means the goroutine can make
// progress, so the suspension signal must not fire.
func TestParkerSignalBeforeParkDoesNotSuspend(t *testing.T) {
	s := newSuspendSignal()
	s.settle = 5 * time.Millisecond
	s.registerBranch()
	s.commitPending(nil)

	p := s.newParker()
	p.signal() // the worker signals and sends before the waiter parks
	time.Sleep(4 * s.settle)
	p.park() // the delayed waiter parks with an outcome already buffered
	time.Sleep(10 * s.settle)
	if s.fired() {
		t.Fatal("suspension fired while a signaled outcome was not received")
	}

	p.received()
	p.park()
	select {
	case <-s.done():
	case <-time.After(5 * time.Second):
		t.Fatal("suspension did not fire once the waiter parked with no signal pending")
	}
}
