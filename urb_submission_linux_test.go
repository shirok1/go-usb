package usb

import (
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestURBSubmissionPublishesCallbackBeforeCompletion(t *testing.T) {
	// Disable the real reaper; the injected submit operation simulates a device
	// completing immediately, before the submit syscall has returned.
	h := &DeviceHandle{reaping: true, reapMap: make(map[uintptr]func(error))}
	for i := 0; i < 1000; i++ {
		id := uintptr(i + 1)
		done := make(chan struct{})
		err := h.submitRegisteredURB(id, func(error) { close(done) }, func() error {
			if _, ok := h.reapMap[id]; !ok {
				t.Fatal("URB submitted without completion callback")
			}
			go func() { h.reapMutex.Lock(); cb := h.reapMap[id]; delete(h.reapMap, id); h.reapMutex.Unlock(); cb(nil) }()
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("completion lost")
		}
	}
	failure := errors.New("submission failed")
	if err := h.submitRegisteredURB(1001, func(error) { t.Error("failed submission completed") }, func() error { return failure }); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if len(h.reapMap) != 0 {
		t.Fatal("failed submission retained a callback")
	}
}

func TestFailedURBSubmissionWakesWaiters(t *testing.T) {
	// An invalid fd exercises the real submit syscall without touching hardware.
	h := &DeviceHandle{fd: -1, reaping: true, reapMap: make(map[uintptr]func(error))}
	generic, err := h.NewBulkTransfer(0x81, 64)
	if err != nil {
		t.Fatal(err)
	}
	iso, err := h.NewIsochronousTransfer(0x81, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	bulk, err := h.NewAsyncBulkTransfer(0x81, 64)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		submit func() error
		wait   func() error
	}{
		{"generic", generic.Submit, generic.Wait},
		{"isochronous", iso.Submit, iso.Wait},
		{"bulk", bulk.Submit, func() error { _, e := bulk.Wait(); return e }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Retry proves failed submission does not leave a stale submitted flag.
			for attempt := 0; attempt < 2; attempt++ {
				if err := tc.submit(); !errors.Is(err, syscall.EBADF) {
					t.Fatalf("Submit: %v", err)
				}
				done := make(chan error, 1)
				go func() { done <- tc.wait() }()
				select {
				case err := <-done:
					if !errors.Is(err, syscall.EBADF) {
						t.Fatalf("Wait: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("waiter blocked after failed submission")
				}
				if len(h.reapMap) != 0 {
					t.Fatal("stale registration")
				}
			}
		})
	}
}

func TestCancelSynchronizesWithCompletion(t *testing.T) {
	h := &DeviceHandle{fd: -1}
	generic := &AsyncTransfer{handle: h, urb: &URB{}, reapCond: sync.NewCond(&sync.Mutex{})}
	iso := &IsochronousTransfer{handle: h, urb: &URB{}, reapCond: sync.NewCond(&sync.Mutex{})}
	for _, tc := range []struct {
		name      string
		cond      *sync.Cond
		submitted *bool
		cancel    func() error
	}{
		{"generic", generic.reapCond, &generic.submitted, generic.Cancel},
		{"isochronous", iso.reapCond, &iso.submitted, iso.Cancel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			done := make(chan struct{})
			go func() {
				defer close(done)
				for i := 0; i < 1000; i++ {
					tc.cond.L.Lock()
					*tc.submitted = i%2 == 0
					tc.cond.L.Unlock()
				}
			}()
			for i := 0; i < 1000; i++ {
				_ = tc.cancel()
			}
			<-done
		})
	}
}

func TestDuplicateSubmissionDoesNotResetPendingURB(t *testing.T) {
	h := &DeviceHandle{fd: -1}
	generic := &AsyncTransfer{handle: h, submitted: true, urb: &URB{}, reapCond: sync.NewCond(&sync.Mutex{})}
	iso := &IsochronousTransfer{handle: h, submitted: true, urb: &URB{}, reapCond: sync.NewCond(&sync.Mutex{})}
	bulk := &AsyncBulkTransfer{handle: h, submitted: true, urb: &URB{}, reapCond: sync.NewCond(&sync.Mutex{})}
	for _, tc := range []struct {
		name   string
		submit func() error
	}{
		{"generic", generic.Submit}, {"isochronous", iso.Submit}, {"bulk", bulk.Submit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A pending URB can still have status zero; status is not a submitted flag.
			if e := tc.submit(); e == nil || e.Error() != "transfer already submitted" {
				t.Fatalf("duplicate submission reached ioctl: %v", e)
			}
		})
	}
}
