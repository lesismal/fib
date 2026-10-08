package udpbatch

// SetGSOOff makes Send leave out UDP_SEGMENT, as it does once the kernel has
// refused a segmented send, and reports what it was. For tests.
func SetGSOOff(v bool) (prev bool) { return gsoOff.Swap(v) }

// GSOOff reports whether Send has stopped using UDP_SEGMENT.
func GSOOff() bool { return gsoOff.Load() }
