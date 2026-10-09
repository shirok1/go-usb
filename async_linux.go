package usb

import (
	"fmt"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// AsyncTransfer represents an asynchronous USB transfer
type AsyncTransfer struct {
	handle       *DeviceHandle
	endpoint     uint8
	transferType TransferType
	buffer       []byte
	timeout      time.Duration
	actualLength int
	isoPackets   []IsoPacket

	// URB fields
	urb       *URB
	urbBuffer []byte // Holds URB struct (+ iso packets if needed)
	submitted bool

	// Auto-reaping support
	reapErr  error
	reaped   bool
	reapCond *sync.Cond
}

// IsoPacket represents an isochronous packet
type IsoPacket struct {
	Length       int
	ActualLength int
	Status       int
}

// NewBulkTransfer creates a new bulk transfer
func (h *DeviceHandle) NewBulkTransfer(endpoint uint8, bufferSize int) (*AsyncTransfer, error) {
	return h.newAsyncTransfer(endpoint, TransferTypeBulk, bufferSize, 0)
}

// NewInterruptTransfer creates a new interrupt transfer
func (h *DeviceHandle) NewInterruptTransfer(endpoint uint8, bufferSize int) (*AsyncTransfer, error) {
	return h.newAsyncTransfer(endpoint, TransferTypeInterrupt, bufferSize, 0)
}

// NewControlTransfer creates a new control transfer
func (h *DeviceHandle) NewControlTransfer(bufferSize int) (*AsyncTransfer, error) {
	return h.newAsyncTransfer(0, TransferTypeControl, bufferSize, 0)
}

// newAsyncTransfer creates a new asynchronous transfer
func (h *DeviceHandle) newAsyncTransfer(endpoint uint8, transferType TransferType, bufferSize int, isoPackets int) (*AsyncTransfer, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return nil, ErrDeviceNotFound
	}

	transfer := &AsyncTransfer{
		handle:       h,
		endpoint:     endpoint,
		transferType: transferType,
		buffer:       make([]byte, bufferSize),
		timeout:      5 * time.Second,
		reapCond:     sync.NewCond(&sync.Mutex{}),
	}

	// Calculate URB size
	urbSize := unsafe.Sizeof(URB{})
	if transferType == TransferTypeIsochronous && isoPackets > 0 {
		// Add space for iso packet descriptors
		urbSize += uintptr(isoPackets) * unsafe.Sizeof(IsoPacketDescriptor{})
		transfer.isoPackets = make([]IsoPacket, isoPackets)
	}

	// Allocate URB buffer
	transfer.urbBuffer = make([]byte, urbSize)
	transfer.urb = (*URB)(unsafe.Pointer(&transfer.urbBuffer[0]))

	// Set up URB fields
	switch transferType {
	case TransferTypeBulk:
		transfer.urb.Type = USBDEVFS_URB_TYPE_BULK
	case TransferTypeInterrupt:
		transfer.urb.Type = USBDEVFS_URB_TYPE_INTERRUPT
	case TransferTypeControl:
		transfer.urb.Type = USBDEVFS_URB_TYPE_CONTROL
	case TransferTypeIsochronous:
		transfer.urb.Type = USBDEVFS_URB_TYPE_ISO
		transfer.urb.NumberOfPackets = int32(isoPackets)
		transfer.urb.Flags = USBDEVFS_URB_ISO_ASAP
	}

	transfer.urb.Endpoint = endpoint
	transfer.urb.Buffer = unsafe.Pointer(&transfer.buffer[0])
	transfer.urb.BufferLength = int32(bufferSize)

	return transfer, nil
}

// SetTimeout sets the transfer timeout
func (t *AsyncTransfer) SetTimeout(timeout time.Duration) {
	t.timeout = timeout
}

// Status returns the transfer status
func (t *AsyncTransfer) Status() TransferStatus {
	t.waitForReaping()

	if t.reapErr != nil {
		if t.urb.Status == -int32(syscall.ETIMEDOUT) {
			return TransferTimedOut
		}
		return TransferError
	}
	return TransferCompleted
}

// ActualLength returns actual bytes transferred
func (t *AsyncTransfer) ActualLength() int {
	t.waitForReaping()
	return t.actualLength
}

// Buffer returns the transfer buffer
func (t *AsyncTransfer) Buffer() []byte {
	t.waitForReaping()
	return t.buffer[:t.actualLength]
}

// IsoPackets returns isochronous packets
func (t *AsyncTransfer) IsoPackets() []IsoPacket {
	t.waitForReaping()
	return t.isoPackets
}

// Submit submits the transfer for execution
func (t *AsyncTransfer) Submit() error {
	t.handle.mu.RLock()
	defer t.handle.mu.RUnlock()

	if t.handle.closed {
		return ErrDeviceNotFound
	}

	t.reapCond.L.Lock()
	defer t.reapCond.L.Unlock()
	if t.submitted {
		return fmt.Errorf("transfer already submitted")
	}
	t.submitted = true
	t.reaped = false
	t.reapErr = nil

	// Reset URB fields
	t.urb.Status = 0
	t.urb.ActualLength = 0
	t.urb.ErrorCount = 0

	// Set up iso packets if needed
	if t.transferType == TransferTypeIsochronous && len(t.isoPackets) > 0 {
		isoPackets := unsafe.Slice((*IsoPacketDescriptor)(unsafe.Pointer(
			uintptr(unsafe.Pointer(t.urb))+unsafe.Sizeof(URB{}))), len(t.isoPackets))
		for i := range t.isoPackets {
			isoPackets[i].Length = uint32(t.isoPackets[i].Length)
			isoPackets[i].ActualLength = 0
			isoPackets[i].Status = 0
		}
	}

	err := t.handle.submitRegisteredURB(uintptr(unsafe.Pointer(t.urb)), func(err error) {
		// Process URB completion
		t.reapCond.L.Lock()
		defer t.reapCond.L.Unlock()

		t.reapErr = err

		if err == nil {
			t.actualLength = int(t.urb.ActualLength)

			// Update iso packets if needed
			if t.transferType == TransferTypeIsochronous && len(t.isoPackets) > 0 {
				isoPackets := unsafe.Slice((*IsoPacketDescriptor)(unsafe.Pointer(
					uintptr(unsafe.Pointer(t.urb))+unsafe.Sizeof(URB{}))), len(t.isoPackets))
				for i := range t.isoPackets {
					t.isoPackets[i].ActualLength = int(isoPackets[i].ActualLength)
					t.isoPackets[i].Status = int(isoPackets[i].Status)
				}
			}
		}

		// Clear submitted flag to allow resubmission
		t.submitted = false
		t.reaped = true
		t.reapCond.Broadcast()
	}, func() error {
		_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(t.handle.fd), USBDEVFS_SUBMITURB, uintptr(unsafe.Pointer(t.urb)))
		if errno != 0 {
			return fmt.Errorf("failed to submit URB: %w", errno)
		}
		return nil
	})
	if err != nil {
		t.submitted = false
		t.reaped = true
		t.reapErr = err
		t.reapCond.Broadcast()
		return err
	}

	return nil
}

// Cancel cancels the transfer
func (t *AsyncTransfer) Cancel() error {
	t.reapCond.L.Lock()
	submitted := t.submitted
	t.reapCond.L.Unlock()
	if !submitted {
		return fmt.Errorf("transfer not submitted")
	}
	t.handle.mu.RLock()
	defer t.handle.mu.RUnlock()

	if t.handle.closed {
		return ErrDeviceNotFound
	}

	// Discard the URB
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(t.handle.fd),
		USBDEVFS_DISCARDURB,
		uintptr(unsafe.Pointer(t.urb)),
	)

	if errno != 0 && errno != syscall.EINVAL {
		return fmt.Errorf("failed to cancel URB: %v", errno)
	}

	return nil
}

// NewAsyncTransfer creates a new asynchronous transfer.
//
// Deprecated: use DeviceHandle.NewBulkTransfer, NewInterruptTransfer or
// NewControlTransfer, which report allocation errors.
func NewAsyncTransfer(handle *DeviceHandle, endpoint uint8, transferType TransferType, bufferSize int) *AsyncTransfer {
	transfer, err := handle.newAsyncTransfer(endpoint, transferType, bufferSize, 0)
	if err != nil {
		return nil
	}
	return transfer
}

// IsCompleted reports whether the transfer has been reaped.
//
// Unlike Status or ActualLength it never blocks, so it is safe to poll.
func (t *AsyncTransfer) IsCompleted() bool {
	if t.reapCond == nil {
		return false
	}
	t.reapCond.L.Lock()
	defer t.reapCond.L.Unlock()
	return t.reaped
}

// waitForReaping waits for the transfer to be reaped
func (t *AsyncTransfer) waitForReaping() {
	t.reapCond.L.Lock()
	defer t.reapCond.L.Unlock()

	for !t.reaped {
		t.reapCond.Wait()
	}
}

// Wait blocks until the transfer completes, honoring the timeout set by
// SetTimeout (5s by default if never called) exactly like WaitWithTimeout
// would with that same value -- usbfs URBs have no kernel-side timeout of
// their own, so nothing else would ever make this return on a transfer
// that genuinely never completes.
func (t *AsyncTransfer) Wait() error {
	return t.WaitWithTimeout(t.timeout)
}

// WaitWithTimeout waits for transfer completion with timeout
func (t *AsyncTransfer) WaitWithTimeout(timeout time.Duration) error {
	done := make(chan error, 1)

	go func() {
		t.waitForReaping()
		done <- t.reapErr
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		// Try to cancel the transfer
		t.Cancel()
		return ErrTimeout
	}
}

// Fill fills the buffer with data (for OUT transfers)
func (t *AsyncTransfer) Fill(data []byte) error {
	if len(data) > len(t.buffer) {
		return fmt.Errorf("data too large for buffer")
	}
	copy(t.buffer, data)
	// Update the buffer length for the actual data size
	t.urb.BufferLength = int32(len(data))
	return nil
}

// SetIsoPacketLengths sets the length for all isochronous packets
func (t *AsyncTransfer) SetIsoPacketLengths(length int) {
	for i := range t.isoPackets {
		t.isoPackets[i].Length = length
	}
}
