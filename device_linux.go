package usb

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

const (
	USBDEVFS_CONTROL          = 0xc0185500
	USBDEVFS_BULK             = 0xc0185502
	USBDEVFS_CLAIMINTERFACE   = 0x8004550f
	USBDEVFS_RELEASEINTERFACE = 0x80045510
	USBDEVFS_SETINTERFACE     = 0x80085504
	USBDEVFS_CLEAR_HALT       = 0x80045515
	USBDEVFS_RESETEP          = 0x80045503
	USBDEVFS_SETCONFIGURATION = 0x80045505
	USBDEVFS_GETDRIVER        = 0x41045508
	USBDEVFS_SUBMITURB        = 0x8038550a
	USBDEVFS_DISCARDURB       = 0x0000550b
	USBDEVFS_REAPURB          = 0x4008550c
	USBDEVFS_REAPURBNDELAY    = 0x4008550d
	USBDEVFS_DISCONNECT       = 0x00005516
	USBDEVFS_CONNECT          = 0x00005517
	USBDEVFS_DISCONNECT_CLAIM = 0x8108551b
	USBDEVFS_IOCTL            = 0xc0105512
	USBDEVFS_GET_CAPABILITIES = 0x8004551a
	USBDEVFS_ALLOC_STREAMS    = 0x8008551c
	USBDEVFS_FREE_STREAMS     = 0x8008551d
	USBDEVFS_GET_SPEED        = 0x8004551f
)

type Device struct {
	Path          string
	Bus           uint8
	Address       uint8
	Port          uint8
	Speed         Speed
	ParentHubAddr uint8 // USB address of the parent hub (0 = unknown / root hub)
	Descriptor    DeviceDescriptor
	Configs       []RawConfigDescriptor
	SysfsStrings  *SysfsStrings
}

// SysfsStrings is declared in types_common.go as an alias for DeviceStrings so
// that every platform exposes the same type.

type DeviceHandle struct {
	device        *Device
	fd            int
	claimedIfaces map[uint8]bool
	mu            sync.RWMutex
	closed        bool
	wrapped       bool

	// Reaper state for isochronous transfers
	reapMutex sync.Mutex
	reapMap   map[uintptr]func(error) // URB ptr -> completion callback
	reaping   bool                    // Is reaper running?
	reapDone  chan struct{}           // Signals reaper has stopped

	// reapWakeR/reapWakeW are a pipe used solely to interrupt reapLoop's
	// epoll_wait immediately when Close sets h.closed -- see reapLoop's own
	// comment for why a bare epoll_wait on the device fd alone isn't enough.
	reapWakeR, reapWakeW int
}

func (d *Device) Open() (*DeviceHandle, error) {
	fd, err := syscall.Open(d.Path, syscall.O_RDWR, 0)
	if err != nil {
		if err == syscall.EACCES {
			return nil, ErrPermissionDenied
		}
		return nil, fmt.Errorf("failed to open device: %w", err)
	}

	return &DeviceHandle{
		device:        d,
		fd:            fd,
		claimedIfaces: make(map[uint8]bool),
		closed:        false,
		reapMap:       make(map[uintptr]func(error)),
	}, nil
}

func (h *DeviceHandle) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	reapDone := h.reapDone
	wakeW := h.reapWakeW
	h.mu.Unlock()

	// Cancel all pending URBs so their callbacks are reported as errors
	// promptly. The kernel would discard any still-outstanding URBs on its
	// own once the fd closes below regardless, but doing it here means
	// callers find out now rather than only once Close finishes.
	h.reapMutex.Lock()
	for urbPtr := range h.reapMap {
		syscall.Syscall(
			syscall.SYS_IOCTL,
			uintptr(h.fd),
			USBDEVFS_DISCARDURB,
			urbPtr,
		)
	}
	h.reapMutex.Unlock()

	if reapDone != nil {
		// Wake reapLoop's epoll_wait immediately -- it is asleep waiting
		// for either a reapable URB or this signal, and has no other way
		// to notice h.closed once blocked in epoll_wait(-1).
		syscall.Write(wakeW, []byte{0})
		<-reapDone
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for iface := range h.claimedIfaces {
		h.releaseInterfaceInternal(iface)
	}

	return syscall.Close(h.fd)
}

// submitRegisteredURB publishes the callback before submitting to usbfs. The
// reaper uses the same mutex, so even an immediate completion has a callback.
// Callers hold h.mu.RLock and initialize transfer state before calling this.
func (h *DeviceHandle) submitRegisteredURB(urbPtr uintptr, callback func(error), submit func() error) error {
	h.reapMutex.Lock()
	defer h.reapMutex.Unlock()

	if !h.reaping {
		var fds [2]int
		if err := syscall.Pipe2(fds[:], syscall.O_CLOEXEC|syscall.O_NONBLOCK); err != nil {
			return fmt.Errorf("failed to create reap wake pipe: %w", err)
		}
		h.reapWakeR, h.reapWakeW = fds[0], fds[1]
		h.reaping = true
		h.reapDone = make(chan struct{})
		go h.reapLoop()
	}

	h.reapMap[urbPtr] = callback
	if err := submit(); err != nil {
		delete(h.reapMap, urbPtr)
		return err
	}
	return nil
}

// reapLoop reaps completed URBs and notifies waiting transfers.
//
// Blocks in epoll_wait on two fds: the device fd itself, and reapWakeR, a
// pipe Close writes a byte to specifically to interrupt this wait
// immediately. This mirrors libusb's own linux_usbfs.c exactly: it too
// reaps via the non-blocking USBDEVFS_REAPURBNDELAY only after poll/epoll
// reports the device fd ready, and uses a self-pipe/eventfd (see its
// events_posix.c) to interrupt that wait for cancellation, rather than
// libusb's *earlier* history of a plain blocking USBDEVFS_REAPURB.
//
// Confirmed against the actual kernel source (drivers/usb/core/devio.c,
// usbdev_poll): usbfs reports a reapable URB via EPOLLOUT, not EPOLLIN --
// an easy-to-get-backwards kernel quirk, but the same one libusb's own
// usbi_add_event_source(ctx, fd, POLLOUT) call relies on, so this matches
// it deliberately rather than by accident.
//
// This replaced an earlier version of this fix that polled
// USBDEVFS_REAPURBNDELAY on a 5ms timer instead of using epoll. That
// avoided the real deadlock this whole mechanism exists to fix (see git
// history), but added up to 5ms of latency to every completion
// notification even under load -- enough to matter for a continuous
// high-throughput capture device (e.g. a logic analyzer) with a shallow
// in-flight buffer count, where 5ms of added latency before the host
// notices a buffer is free to resubmit can starve the device's FIFO into
// overflowing. This version has no such added latency: epoll_wait returns
// as soon as the kernel actually has something ready, same as libusb.
func (h *DeviceHandle) reapLoop() {
	defer func() {
		syscall.Close(h.reapWakeR)
		syscall.Close(h.reapWakeW)
		if h.reapDone != nil {
			close(h.reapDone)
		}
	}()

	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		h.failReaping(fmt.Errorf("epoll_create1: %w", err))
		return
	}
	defer syscall.Close(epfd)

	devEvent := syscall.EpollEvent{Events: syscall.EPOLLOUT, Fd: int32(h.fd)}
	if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, h.fd, &devEvent); err != nil {
		h.failReaping(fmt.Errorf("epoll_ctl(device fd): %w", err))
		return
	}
	wakeEvent := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(h.reapWakeR)}
	if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, h.reapWakeR, &wakeEvent); err != nil {
		h.failReaping(fmt.Errorf("epoll_ctl(wake pipe): %w", err))
		return
	}

	events := make([]syscall.EpollEvent, 2)
	for {
		h.mu.RLock()
		closed := h.closed
		h.mu.RUnlock()

		if closed {
			h.reapMutex.Lock()
			for _, callback := range h.reapMap {
				callback(ErrDeviceNotFound)
			}
			h.reapMap = nil
			h.reaping = false
			h.reapMutex.Unlock()
			return
		}

		n, err := syscall.EpollWait(epfd, events, -1)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			h.failReaping(fmt.Errorf("epoll_wait: %w", err))
			return
		}

		// A wakeup on the pipe only means "go recheck h.closed"; drain the
		// byte Close wrote so the fd stops reporting readable, but the
		// real state change is re-read at the top of the loop either way.
		for i := 0; i < n; i++ {
			if events[i].Fd == int32(h.reapWakeR) {
				var buf [8]byte
				syscall.Read(h.reapWakeR, buf[:])
			}
		}

		if !h.drainReapedURBs() {
			return
		}
	}
}

// failReaping reports err to every pending callback and stops reaping.
// Used for reapLoop setup/wait failures that leave nothing left to reap.
func (h *DeviceHandle) failReaping(err error) {
	h.reapMutex.Lock()
	for _, callback := range h.reapMap {
		callback(err)
	}
	h.reapMap = make(map[uintptr]func(error))
	h.reaping = false
	h.reapMutex.Unlock()
}

// drainReapedURBs calls USBDEVFS_REAPURBNDELAY repeatedly until nothing
// more is ready (EAGAIN), notifying each URB's registered callback as it
// goes. usbfs can have more than one URB ready at a time, so this drains
// all of them per epoll wakeup rather than needing one epoll_wait round
// trip per URB -- the same thing libusb's reap_for_handle does.
//
// Returns false if a fatal ioctl error stopped reaping entirely (already
// reported via failReaping); true otherwise, including the ordinary case
// where there was simply nothing left to reap.
func (h *DeviceHandle) drainReapedURBs() bool {
	for {
		var reapedURB *URB

		_, _, errno := syscall.Syscall(
			syscall.SYS_IOCTL,
			uintptr(h.fd),
			USBDEVFS_REAPURBNDELAY,
			uintptr(unsafe.Pointer(&reapedURB)),
		)
		if errno == syscall.EAGAIN {
			return true
		}
		if errno == syscall.EINTR {
			continue
		}
		if errno != 0 {
			h.failReaping(fmt.Errorf("reaper failed: %v", errno))
			return false
		}

		h.reapMutex.Lock()
		callback, ok := h.reapMap[uintptr(unsafe.Pointer(reapedURB))]
		if !ok {
			// URB was reaped but not registered - can happen during shutdown
			h.reapMutex.Unlock()
			continue
		}
		delete(h.reapMap, uintptr(unsafe.Pointer(reapedURB)))
		h.reapMutex.Unlock()

		var err error
		if reapedURB.Status != 0 {
			err = fmt.Errorf("URB completed with status: %d", reapedURB.Status)
		}
		callback(err)
	}
}

func (h *DeviceHandle) Descriptor() DeviceDescriptor {
	return h.device.Descriptor
}

func (h *DeviceHandle) Configuration() (int, error) {
	buf := make([]byte, 1)

	ctrl := usbCtrlRequest{
		RequestType: 0x80,
		Request:     0x08,
		Value:       0,
		Index:       0,
		Length:      uint16(len(buf)),
		Data:        unsafe.Pointer(&buf[0]),
	}

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_CONTROL, uintptr(unsafe.Pointer(&ctrl)))
	if errno != 0 {
		return 0, errno
	}

	return int(buf[0]), nil
}

func (h *DeviceHandle) SetConfiguration(config int) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return ErrDeviceNotFound
	}

	cfg := uint32(config)
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_SETCONFIGURATION, uintptr(unsafe.Pointer(&cfg)))
	if errno != 0 {
		return errno
	}

	return nil
}

// ConfigDescriptorByValue gets the parsed configuration descriptor by index
// This is equivalent to libusb_get_config_descriptor
func (h *DeviceHandle) ConfigDescriptorByValue(index uint8) (*ConfigDescriptor, error) {
	data, err := h.RawConfigDescriptor(index)
	if err != nil {
		return nil, err
	}

	config := &ConfigDescriptor{}
	err = config.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// RawConfigDescriptor gets the raw configuration descriptor data by index
func (h *DeviceHandle) RawConfigDescriptor(index uint8) ([]byte, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return nil, ErrDeviceNotFound
	}

	// First get the config descriptor header to know the total length
	buf := make([]byte, 9)

	ctrl := usbCtrlRequest{
		RequestType: 0x80, // Device to host
		Request:     USB_REQ_GET_DESCRIPTOR,
		Value:       (USB_DT_CONFIG << 8) | uint16(index),
		Index:       0,
		Length:      9,
		Data:        unsafe.Pointer(&buf[0]),
	}

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_CONTROL, uintptr(unsafe.Pointer(&ctrl)))
	if errno != 0 {
		return nil, fmt.Errorf("failed to get config descriptor header: %w", errno)
	}

	// Parse total length from the header
	totalLength := binary.LittleEndian.Uint16(buf[2:4])

	// Now get the full descriptor
	fullBuf := make([]byte, totalLength)
	ctrl.Length = totalLength
	ctrl.Data = unsafe.Pointer(&fullBuf[0])

	_, _, errno = syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_CONTROL, uintptr(unsafe.Pointer(&ctrl)))
	if errno != 0 {
		return nil, fmt.Errorf("failed to get full config descriptor: %w", errno)
	}

	return fullBuf, nil
}

// usbfsDisconnectClaim matches the kernel's usbfs_disconnect_claim struct
type usbfsDisconnectClaim struct {
	Interface uint32
	Flags     uint32
	Driver    [256]byte
}

func (h *DeviceHandle) ClaimInterface(iface uint8) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return ErrDeviceNotFound
	}

	if h.claimedIfaces[iface] {
		return nil
	}

	// Use DISCONNECT_CLAIM with EXCEPT_DRIVER flag to atomically disconnect
	// competing drivers and claim. This matches libusb's behavior.
	dc := usbfsDisconnectClaim{
		Interface: uint32(iface),
		Flags:     0x02, // USBFS_DISCONNECT_CLAIM_EXCEPT_DRIVER
	}
	copy(dc.Driver[:], "usbfs")

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_DISCONNECT_CLAIM, uintptr(unsafe.Pointer(&dc)))
	if errno == 0 {
		h.claimedIfaces[iface] = true
		return nil
	}

	if h.shouldTreatClaimBusyAsSuccess(errno) {
		h.claimedIfaces[iface] = true
		return nil
	}

	// Fallback to simple claim if DISCONNECT_CLAIM not supported
	if errno == syscall.ENOTTY || errno == syscall.EINVAL {
		ifaceNum := uint32(iface)
		_, _, errno = syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_CLAIMINTERFACE, uintptr(unsafe.Pointer(&ifaceNum)))
		if errno != 0 {
			if h.shouldTreatClaimBusyAsSuccess(errno) {
				h.claimedIfaces[iface] = true
				return nil
			}
			return errno
		}
		h.claimedIfaces[iface] = true
		return nil
	}

	return errno
}

func (h *DeviceHandle) shouldTreatClaimBusyAsSuccess(errno syscall.Errno) bool {
	return h.wrapped && errno == syscall.EBUSY
}

func (h *DeviceHandle) ReleaseInterface(iface uint8) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return ErrDeviceNotFound
	}

	return h.releaseInterfaceInternal(iface)
}

func (h *DeviceHandle) releaseInterfaceInternal(iface uint8) error {
	if !h.claimedIfaces[iface] {
		return nil
	}

	ifaceNum := uint32(iface)
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_RELEASEINTERFACE, uintptr(unsafe.Pointer(&ifaceNum)))
	if errno != 0 {
		return errno
	}

	delete(h.claimedIfaces, iface)
	return nil
}

func (h *DeviceHandle) SetInterfaceAltSetting(iface uint8, altSetting uint8) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return ErrDeviceNotFound
	}

	if !h.claimedIfaces[iface] {
		return fmt.Errorf("interface %d not claimed", iface)
	}

	setIface := struct {
		Interface  uint32
		AltSetting uint32
	}{
		Interface:  uint32(iface),
		AltSetting: uint32(altSetting),
	}

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_SETINTERFACE, uintptr(unsafe.Pointer(&setIface)))
	if errno != 0 {
		return errno
	}

	return nil
}

func (h *DeviceHandle) ClearHalt(endpoint uint8) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return ErrDeviceNotFound
	}

	ep := uint32(endpoint)
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_CLEAR_HALT, uintptr(unsafe.Pointer(&ep)))
	if errno != 0 {
		return errno
	}

	return nil
}

func (h *DeviceHandle) DetachKernelDriver(iface uint8) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return ErrDeviceNotFound
	}

	// Use USBDEVFS_DISCONNECT to detach kernel driver (matches libusb)
	ifaceNum := uint32(iface)
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_DISCONNECT, uintptr(unsafe.Pointer(&ifaceNum)))
	if errno != 0 {
		if errno == syscall.ENODATA || errno == syscall.ENOENT || errno == syscall.ENOTTY {
			return nil
		}
		return errno
	}

	return nil
}

func (h *DeviceHandle) AttachKernelDriver(iface uint8) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return ErrDeviceNotFound
	}

	// Use USBDEVFS_CONNECT to re-attach kernel driver
	ifaceNum := uint32(iface)
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_CONNECT, uintptr(unsafe.Pointer(&ifaceNum)))
	if errno != 0 {
		// ENODATA means driver was not previously bound
		// EBUSY means driver is already attached
		// Both are acceptable outcomes
		if errno == syscall.ENODATA || errno == syscall.EBUSY {
			return nil
		}
		return errno
	}

	return nil
}

// Status gets device, interface, or endpoint status
func (h *DeviceHandle) Status(requestType uint8, index uint16) (uint16, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return 0, ErrDeviceNotFound
	}

	buf := make([]byte, 2)

	ctrl := usbCtrlRequest{
		RequestType: requestType, // 0x80 for device, 0x81 for interface, 0x82 for endpoint
		Request:     USB_REQ_GET_STATUS,
		Value:       0,
		Index:       index,
		Length:      2,
		Data:        unsafe.Pointer(&buf[0]),
	}

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_CONTROL, uintptr(unsafe.Pointer(&ctrl)))
	if errno != 0 {
		return 0, errno
	}

	return binary.LittleEndian.Uint16(buf), nil
}

// ClearFeature clears a feature on device, interface, or endpoint
func (h *DeviceHandle) ClearFeature(requestType uint8, feature uint16, index uint16) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return ErrDeviceNotFound
	}

	ctrl := usbCtrlRequest{
		RequestType: requestType, // 0x00 for device, 0x01 for interface, 0x02 for endpoint
		Request:     USB_REQ_CLEAR_FEATURE,
		Value:       feature,
		Index:       index,
		Length:      0,
		Data:        nil,
	}

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_CONTROL, uintptr(unsafe.Pointer(&ctrl)))
	if errno != 0 {
		return errno
	}

	return nil
}

// SetFeature sets a feature on device, interface, or endpoint
func (h *DeviceHandle) SetFeature(requestType uint8, feature uint16, index uint16) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return ErrDeviceNotFound
	}

	ctrl := usbCtrlRequest{
		RequestType: requestType, // 0x00 for device, 0x01 for interface, 0x02 for endpoint
		Request:     USB_REQ_SET_FEATURE,
		Value:       feature,
		Index:       index,
		Length:      0,
		Data:        nil,
	}

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_CONTROL, uintptr(unsafe.Pointer(&ctrl)))
	if errno != 0 {
		return errno
	}

	return nil
}

// Interface gets the alternate setting of an interface
func (h *DeviceHandle) Interface(iface uint8) (uint8, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return 0, ErrDeviceNotFound
	}

	buf := make([]byte, 1)

	ctrl := usbCtrlRequest{
		RequestType: 0x81, // Interface recipient
		Request:     USB_REQ_GET_INTERFACE,
		Value:       0,
		Index:       uint16(iface),
		Length:      1,
		Data:        unsafe.Pointer(&buf[0]),
	}

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_CONTROL, uintptr(unsafe.Pointer(&ctrl)))
	if errno != 0 {
		return 0, errno
	}

	return buf[0], nil
}

// RawDescriptor gets any descriptor by type and index
func (h *DeviceHandle) RawDescriptor(descType uint8, descIndex uint8, langID uint16, data []byte) (int, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return 0, ErrDeviceNotFound
	}

	var dataPtr unsafe.Pointer
	if len(data) > 0 {
		dataPtr = unsafe.Pointer(&data[0])
	}

	ctrl := usbCtrlRequest{
		RequestType: 0x80, // Device-to-host
		Request:     USB_REQ_GET_DESCRIPTOR,
		Value:       (uint16(descType) << 8) | uint16(descIndex),
		Index:       langID,
		Length:      uint16(len(data)),
		Data:        dataPtr,
	}

	ret, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_CONTROL, uintptr(unsafe.Pointer(&ctrl)))
	if errno != 0 {
		return 0, errno
	}

	return int(ret), nil
}

// SetDescriptor sets a descriptor (rarely used)
func (h *DeviceHandle) SetDescriptor(descType uint8, descIndex uint8, langID uint16, data []byte) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return ErrDeviceNotFound
	}

	var dataPtr unsafe.Pointer
	if len(data) > 0 {
		dataPtr = unsafe.Pointer(&data[0])
	}

	ctrl := usbCtrlRequest{
		RequestType: 0x00, // Host-to-device
		Request:     USB_REQ_SET_DESCRIPTOR,
		Value:       (uint16(descType) << 8) | uint16(descIndex),
		Index:       langID,
		Length:      uint16(len(data)),
		Data:        dataPtr,
	}

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_CONTROL, uintptr(unsafe.Pointer(&ctrl)))
	if errno != 0 {
		return errno
	}

	return nil
}

// SynchFrame synchronizes isochronous transfers (USB 1.1/2.0)
func (h *DeviceHandle) SynchFrame(endpoint uint8) (uint16, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return 0, ErrDeviceNotFound
	}

	buf := make([]byte, 2)

	ctrl := usbCtrlRequest{
		RequestType: 0x82, // Endpoint recipient
		Request:     USB_REQ_SYNCH_FRAME,
		Value:       0,
		Index:       uint16(endpoint),
		Length:      2,
		Data:        unsafe.Pointer(&buf[0]),
	}

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_CONTROL, uintptr(unsafe.Pointer(&ctrl)))
	if errno != 0 {
		return 0, errno
	}

	return binary.LittleEndian.Uint16(buf), nil
}

// Capabilities gets usbfs capabilities (Linux 3.15+)
func (h *DeviceHandle) Capabilities() (uint32, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return 0, ErrDeviceNotFound
	}

	var caps uint32
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_GET_CAPABILITIES, uintptr(unsafe.Pointer(&caps)))
	if errno != 0 {
		return 0, errno
	}

	return caps, nil
}

// Speed gets the device speed
func (h *DeviceHandle) Speed() (uint8, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return 0, ErrDeviceNotFound
	}

	var speed uint32
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_GET_SPEED, uintptr(unsafe.Pointer(&speed)))
	if errno != 0 {
		return 0, errno
	}

	return uint8(speed), nil
}

// AllocStreams allocates bulk streams (USB 3.0+)
func (h *DeviceHandle) AllocStreams(numStreams uint32, endpoints []uint8) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return ErrDeviceNotFound
	}

	streams := struct {
		NumStreams uint32
		NumEps     uint32
		Eps        [30]uint8 // Maximum endpoints per interface
	}{
		NumStreams: numStreams,
		NumEps:     uint32(len(endpoints)),
	}

	copy(streams.Eps[:], endpoints)

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_ALLOC_STREAMS, uintptr(unsafe.Pointer(&streams)))
	if errno != 0 {
		return errno
	}

	return nil
}

// FreeStreams frees bulk streams (USB 3.0+)
func (h *DeviceHandle) FreeStreams(endpoints []uint8) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return ErrDeviceNotFound
	}

	streams := struct {
		NumEps uint32
		Eps    [30]uint8
	}{
		NumEps: uint32(len(endpoints)),
	}

	copy(streams.Eps[:], endpoints)

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_FREE_STREAMS, uintptr(unsafe.Pointer(&streams)))
	if errno != 0 {
		return errno
	}

	return nil
}

// SSEndpointCompanionDescriptor gets the SuperSpeed endpoint companion descriptor for a given endpoint
// This is equivalent to libusb_get_ss_endpoint_companion_descriptor
func (h *DeviceHandle) SSEndpointCompanionDescriptor(configIndex uint8, interfaceNumber uint8, altSetting uint8, endpointAddress uint8) (*SuperSpeedEndpointCompanionDescriptor, error) {
	config, err := h.ConfigDescriptorByValue(configIndex)
	if err != nil {
		return nil, err
	}

	altSettingDesc := config.InterfaceAltSetting(interfaceNumber, altSetting)
	if altSettingDesc == nil {
		return nil, fmt.Errorf("interface %d alt setting %d not found", interfaceNumber, altSetting)
	}

	// Find the endpoint and return its companion descriptor if present
	for i := range altSettingDesc.Endpoints {
		if altSettingDesc.Endpoints[i].EndpointAddr == endpointAddress {
			if altSettingDesc.Endpoints[i].SSCompanion != nil {
				return altSettingDesc.Endpoints[i].SSCompanion, nil
			}
			// Search through extra data if companion wasn't parsed
			extra := altSettingDesc.Endpoints[i].Extra
			pos := 0
			for pos+2 <= len(extra) {
				length := int(extra[pos])
				descType := extra[pos+1]

				if length < 2 || pos+length > len(extra) {
					break
				}

				if descType == USB_DT_SS_ENDPOINT_COMPANION {
					if length < 6 {
						return nil, fmt.Errorf("invalid SS endpoint companion descriptor length: %d", length)
					}
					return &SuperSpeedEndpointCompanionDescriptor{
						Length:           extra[pos],
						DescriptorType:   extra[pos+1],
						MaxBurst:         extra[pos+2],
						Attributes:       extra[pos+3],
						BytesPerInterval: binary.LittleEndian.Uint16(extra[pos+4 : pos+6]),
					}, nil
				}

				pos += length
			}
			return nil, fmt.Errorf("SS endpoint companion descriptor not found for endpoint %02x", endpointAddress)
		}
	}

	return nil, fmt.Errorf("endpoint %02x not found", endpointAddress)
}

// SSUSBDeviceCapabilityDescriptor gets the SuperSpeed USB device capability descriptor
// This is equivalent to libusb_get_ss_usb_device_capability_descriptor
func (h *DeviceHandle) SSUSBDeviceCapabilityDescriptor() (*SuperSpeedUSBCapability, error) {
	// Read the full BOS descriptor once
	buf := make([]byte, 1024) // Start with reasonable size
	n, err := h.RawDescriptor(USB_DT_BOS, 0, 0, buf)
	if err != nil || n < 5 {
		return nil, fmt.Errorf("failed to read BOS descriptor: %w", err)
	}

	// Resize buffer to actual data read
	buf = buf[:n]

	// Parse BOS header
	if buf[1] != USB_DT_BOS {
		return nil, fmt.Errorf("not a BOS descriptor")
	}

	totalLength := binary.LittleEndian.Uint16(buf[2:4])
	if int(totalLength) > n {
		// Need to read more data
		buf = make([]byte, totalLength)
		n, err = h.RawDescriptor(USB_DT_BOS, 0, 0, buf)
		if err != nil || n < int(totalLength) {
			return nil, fmt.Errorf("failed to read full BOS descriptor: %w", err)
		}
		buf = buf[:n]
	}

	numDevCaps := buf[4]
	pos := 5 // Start after BOS header

	// Look for SuperSpeed USB capability (type 0x03)
	for i := 0; i < int(numDevCaps) && pos < len(buf); i++ {
		if pos+3 > len(buf) {
			break
		}

		length := int(buf[pos])
		descType := buf[pos+1]
		devCapType := buf[pos+2]

		if length < 3 || pos+length > len(buf) {
			break
		}

		if descType == USB_DT_DEVICE_CAPABILITY && devCapType == 0x03 {
			// Found SuperSpeed USB capability
			if length < 10 {
				return nil, fmt.Errorf("invalid SuperSpeed USB capability length: %d", length)
			}

			return &SuperSpeedUSBCapability{
				Length:                 buf[pos],
				DescriptorType:         buf[pos+1],
				DevCapabilityType:      buf[pos+2],
				Attributes:             buf[pos+3],
				SpeedsSupported:        binary.LittleEndian.Uint16(buf[pos+4 : pos+6]),
				FunctionalitySupported: buf[pos+6],
				U1DevExitLat:           buf[pos+7],
				U2DevExitLat:           binary.LittleEndian.Uint16(buf[pos+8 : pos+10]),
			}, nil
		}

		pos += length
	}

	return nil, fmt.Errorf("SuperSpeed USB capability not found")
}

// USB20ExtensionDescriptor gets the USB 2.0 extension descriptor
// This is equivalent to libusb_get_usb_2_0_extension_descriptor
func (h *DeviceHandle) USB20ExtensionDescriptor() (*USB2ExtensionCapability, error) {
	// Read the full BOS descriptor once
	buf := make([]byte, 1024) // Start with reasonable size
	n, err := h.RawDescriptor(USB_DT_BOS, 0, 0, buf)
	if err != nil || n < 5 {
		return nil, fmt.Errorf("failed to read BOS descriptor: %w", err)
	}

	// Resize buffer to actual data read
	buf = buf[:n]

	// Parse BOS header
	if buf[1] != USB_DT_BOS {
		return nil, fmt.Errorf("not a BOS descriptor")
	}

	totalLength := binary.LittleEndian.Uint16(buf[2:4])
	if int(totalLength) > n {
		// Need to read more data
		buf = make([]byte, totalLength)
		n, err = h.RawDescriptor(USB_DT_BOS, 0, 0, buf)
		if err != nil || n < int(totalLength) {
			return nil, fmt.Errorf("failed to read full BOS descriptor: %w", err)
		}
		buf = buf[:n]
	}

	numDevCaps := buf[4]
	pos := 5 // Start after BOS header

	// Look for USB 2.0 extension capability (type 0x02)
	for i := 0; i < int(numDevCaps) && pos < len(buf); i++ {
		if pos+3 > len(buf) {
			break
		}

		length := int(buf[pos])
		descType := buf[pos+1]
		devCapType := buf[pos+2]

		if length < 3 || pos+length > len(buf) {
			break
		}

		if descType == USB_DT_DEVICE_CAPABILITY && devCapType == 0x02 {
			// Found USB 2.0 extension capability
			if length < 7 {
				return nil, fmt.Errorf("invalid USB 2.0 extension capability length: %d", length)
			}

			return &USB2ExtensionCapability{
				Length:            buf[pos],
				DescriptorType:    buf[pos+1],
				DevCapabilityType: buf[pos+2],
				Attributes:        binary.LittleEndian.Uint32(buf[pos+3 : pos+7]),
			}, nil
		}

		pos += length
	}

	return nil, fmt.Errorf("USB 2.0 extension capability not found")
}

// ReadBOSDescriptor reads the Binary Object Store descriptor (USB 3.0+)
func (h *DeviceHandle) ReadBOSDescriptor() (*BOSDescriptor, []DeviceCapabilityDescriptor, error) {
	// First, get the BOS descriptor header
	buf := make([]byte, 5) // BOS descriptor header is 5 bytes

	n, err := h.RawDescriptor(USB_DT_BOS, 0, 0, buf)
	if err != nil || n < 5 {
		return nil, nil, fmt.Errorf("failed to read BOS descriptor: %w", err)
	}

	// Validate descriptor type
	if buf[1] != USB_DT_BOS {
		return nil, nil, fmt.Errorf("not a BOS descriptor (type: 0x%02x)", buf[1])
	}

	bos := &BOSDescriptor{
		Length:         buf[0],
		DescriptorType: buf[1],
		TotalLength:    binary.LittleEndian.Uint16(buf[2:4]),
		NumDeviceCaps:  buf[4],
	}

	// Validate total length is reasonable (not too small)
	if bos.TotalLength < 5 {
		return nil, nil, fmt.Errorf("invalid BOS total length: %d", bos.TotalLength)
	}
	// Note: TotalLength is uint16, so max value is 65535

	// Now read the full BOS descriptor with all capabilities
	fullBuf := make([]byte, bos.TotalLength)
	n, err = h.RawDescriptor(USB_DT_BOS, 0, 0, fullBuf)
	if err != nil || n < int(bos.TotalLength) {
		return nil, nil, fmt.Errorf("failed to read full BOS descriptor: %w", err)
	}

	// Parse device capabilities
	caps := make([]DeviceCapabilityDescriptor, 0, bos.NumDeviceCaps)
	pos := 5 // Start after BOS header

	for i := 0; i < int(bos.NumDeviceCaps) && pos < len(fullBuf); i++ {
		if pos+3 > len(fullBuf) {
			break
		}

		length := int(fullBuf[pos])

		// Validate descriptor length
		if length < 3 {
			break // Invalid descriptor length
		}
		if pos+length > len(fullBuf) {
			break // Descriptor extends beyond buffer
		}

		cap := DeviceCapabilityDescriptor{
			Length:            fullBuf[pos],
			DescriptorType:    fullBuf[pos+1],
			DevCapabilityType: fullBuf[pos+2],
		}

		caps = append(caps, cap)
		pos += length
	}

	return bos, caps, nil
}

// ReadDeviceQualifierDescriptor reads device qualifier (USB 2.0+)
func (h *DeviceHandle) ReadDeviceQualifierDescriptor() (*DeviceQualifierDescriptor, error) {
	buf := make([]byte, 10)

	n, err := h.RawDescriptor(USB_DT_DEVICE_QUALIFIER, 0, 0, buf)
	if err != nil || n < 10 {
		return nil, fmt.Errorf("failed to read device qualifier: %w", err)
	}

	qual := &DeviceQualifierDescriptor{
		Length:            buf[0],
		DescriptorType:    buf[1],
		USBVersion:        binary.LittleEndian.Uint16(buf[2:4]),
		DeviceClass:       buf[4],
		DeviceSubClass:    buf[5],
		DeviceProtocol:    buf[6],
		MaxPacketSize0:    buf[7],
		NumConfigurations: buf[8],
		Reserved:          buf[9],
	}

	return qual, nil
}

// SetTestMode sets USB test mode (for compliance testing)
func (h *DeviceHandle) SetTestMode(testMode uint8) error {
	return h.SetFeature(0x00, USB_DEVICE_TEST_MODE, uint16(testMode)<<8)
}

func (h *DeviceHandle) Device() *Device {
	return h.device
}

// Fd returns the file descriptor for advanced operations (URB submission, etc.)
// The caller should not close this file descriptor.
func (h *DeviceHandle) Fd() int {
	return h.fd
}

func (h *DeviceHandle) StringDescriptor(index uint8) (string, error) {
	if index == 0 {
		return "", nil
	}

	buf := make([]byte, 256)

	ctrl := usbCtrlRequest{
		RequestType: 0x80,
		Request:     0x06,
		Value:       (0x03 << 8) | uint16(index),
		Index:       0x0409,
		Length:      uint16(len(buf)),
		Data:        unsafe.Pointer(&buf[0]),
	}

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(h.fd), USBDEVFS_CONTROL, uintptr(unsafe.Pointer(&ctrl)))
	if errno != 0 {
		return "", errno
	}

	if buf[0] < 2 {
		return "", fmt.Errorf("invalid string descriptor")
	}

	length := int(buf[0])
	if length > len(buf) {
		length = len(buf)
	}

	result := make([]uint16, 0, (length-2)/2)
	for i := 2; i < length; i += 2 {
		if i+1 < length {
			result = append(result, binary.LittleEndian.Uint16(buf[i:i+2]))
		}
	}

	return string(utf16ToRunes(result)), nil
}

func utf16ToRunes(u16 []uint16) []rune {
	runes := make([]rune, 0, len(u16))
	for _, v := range u16 {
		if v == 0 {
			break
		}
		runes = append(runes, rune(v))
	}
	return runes
}

type usbCtrlRequest struct {
	RequestType uint8
	Request     uint8
	Value       uint16
	Index       uint16
	Length      uint16
	Timeout     uint32
	Data        unsafe.Pointer
}

// WrapSysDevice creates a Device and DeviceHandle from an existing file descriptor.
// This is equivalent to libusb_wrap_sys_device(). The file descriptor must be
// an open USB device file descriptor.
// The DeviceHandle takes ownership of the fd and will close it when Close() is called.
func WrapSysDevice(fd int) (*DeviceHandle, error) {
	if fd < 0 {
		return nil, fmt.Errorf("invalid file descriptor: %d", fd)
	}

	// Verify the fd is valid by attempting a simple ioctl
	var caps uint32
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), USBDEVFS_GET_CAPABILITIES, uintptr(unsafe.Pointer(&caps)))
	if errno != 0 && errno != syscall.ENOTTY {
		// ENOTTY is acceptable for older kernels without capability support
		return nil, fmt.Errorf("file descriptor does not appear to be a USB device: %w", errno)
	}

	// Create a minimal Device structure
	// Path, Bus, and Address may be unknown when created from fd
	device := &Device{
		Path:    fmt.Sprintf("<fd:%d>", fd), // Placeholder path indicating fd origin
		Bus:     0,                          // Unknown bus
		Address: 0,                          // Unknown address
	}

	// Read device descriptor using control transfer
	buf := make([]byte, 18)
	ctrl := usbCtrlRequest{
		RequestType: 0x80,
		Request:     USB_REQ_GET_DESCRIPTOR,
		Value:       USB_DT_DEVICE << 8,
		Index:       0,
		Length:      18,
		Data:        unsafe.Pointer(&buf[0]),
	}

	_, _, errno = syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), USBDEVFS_CONTROL, uintptr(unsafe.Pointer(&ctrl)))
	if errno != 0 {
		return nil, fmt.Errorf("failed to read device descriptor: %w", errno)
	}

	// Parse device descriptor
	device.Descriptor = DeviceDescriptor{
		Length:            buf[0],
		DescriptorType:    buf[1],
		USBVersion:        binary.LittleEndian.Uint16(buf[2:4]),
		DeviceClass:       buf[4],
		DeviceSubClass:    buf[5],
		DeviceProtocol:    buf[6],
		MaxPacketSize0:    buf[7],
		VendorID:          binary.LittleEndian.Uint16(buf[8:10]),
		ProductID:         binary.LittleEndian.Uint16(buf[10:12]),
		DeviceVersion:     binary.LittleEndian.Uint16(buf[12:14]),
		ManufacturerIndex: buf[14],
		ProductIndex:      buf[15],
		SerialNumberIndex: buf[16],
		NumConfigurations: buf[17],
	}

	// Try to determine bus and address from /proc/self/fd if possible
	// This is optional and failure is not an error
	fdPath := fmt.Sprintf("/proc/self/fd/%d", fd)
	if devicePath, err := os.Readlink(fdPath); err == nil {
		if strings.HasPrefix(devicePath, "/dev/bus/usb/") {
			parts := strings.Split(devicePath, "/")
			if len(parts) >= 6 {
				if busNum, err := strconv.Atoi(parts[len(parts)-2]); err == nil && busNum >= 0 && busNum <= 255 {
					device.Bus = uint8(busNum)
				}
				if addrNum, err := strconv.Atoi(parts[len(parts)-1]); err == nil && addrNum >= 0 && addrNum <= 255 {
					device.Address = uint8(addrNum)
				}
				device.Path = devicePath // Use real path if available
			}
		}
	}

	// Create DeviceHandle with the provided fd
	return &DeviceHandle{
		device:        device,
		fd:            fd,
		claimedIfaces: make(map[uint8]bool),
		closed:        false,
		wrapped:       true,
		reapMap:       make(map[uintptr]func(error)),
	}, nil
}

func (h *DeviceHandle) Wrapped() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.wrapped
}
