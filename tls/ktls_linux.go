//go:build linux

package tls

import (
	"encoding/binary"
	"syscall"
	"unsafe"
)

// Linux's kernel TLS (Documentation/networking/tls.rst): the socket takes the
// "tls" upper layer protocol, and then the transmit keys, after which it seals
// every send into records of application data, and a send with a record type
// beside it into a record of that type. Receiving stays with this package.
const (
	tcpULP           = 31
	solTLS           = 282
	tlsTX            = 1
	tlsSetRecordType = 1

	tlsVersion12  = 0x0303
	tlsVersion13  = 0x0304
	cipherGCM128  = 51
	cipherGCM256  = 52
	gcmSaltLen    = 4
	gcmIVLen      = 8
	gcmRecSeqLen  = 8
	cryptoInfoLen = 4
)

// kernelTLSFits reports whether the kernel's TLS can take over sealing under
// k: AES-GCM, of TLS 1.3 or 1.2.
func kernelTLSFits(k *recordKeys) bool {
	return !kernelTLSMissing.Load() && (k.mode == modeGCM13 || k.mode == modeGCM12) &&
		(len(k.key) == 16 || len(k.key) == 32)
}

// startKernelTLS hands the sealing of what is sent on fd to the kernel, under
// k from its next sequence number on. On a socket it has been handed already,
// it hands the kernel k in place of the keys it had, as a KeyUpdate does.
func startKernelTLS(fd uintptr, k *recordKeys) error {
	err := syscall.SetsockoptString(int(fd), syscall.IPPROTO_TCP, tcpULP, "tls")
	switch err {
	case nil, syscall.EEXIST:
	case syscall.ENOENT, syscall.ENOPROTOOPT, syscall.EPERM:
		// No tls module, and none this process may load.
		kernelTLSMissing.Store(true)
		return err
	default:
		return err
	}
	return syscall.SetsockoptString(int(fd), solTLS, tlsTX, string(cryptoInfo(k)))
}

// cryptoInfo is linux/tls.h's tls12_crypto_info_aes_gcm_128 or _256 for k:
// the version and cipher, then the explicit part of the nonce, the key, the
// salt and the sequence number of the next record. TLS 1.3's nonce is the
// salt and the explicit part, XORed with the sequence number; TLS 1.2's
// explicit part is sent in each record, and is the sequence number here as it
// is in records this package seals.
func cryptoInfo(k *recordKeys) []byte {
	info := make([]byte, 0, cryptoInfoLen+gcmIVLen+len(k.key)+gcmSaltLen+gcmRecSeqLen)
	version, cipher := uint16(tlsVersion12), uint16(cipherGCM128)
	if k.tls13() {
		version = tlsVersion13
	}
	if len(k.key) == 32 {
		cipher = cipherGCM256
	}
	info = binary.NativeEndian.AppendUint16(info, version)
	info = binary.NativeEndian.AppendUint16(info, cipher)
	if k.tls13() {
		info = append(info, k.iv[gcmSaltLen:]...)
	} else {
		info = binary.BigEndian.AppendUint64(info, k.seq)
	}
	info = append(info, k.key...)
	info = append(info, k.iv[:gcmSaltLen]...)
	return binary.BigEndian.AppendUint64(info, k.seq)
}

// sendKernelRecord writes data on fd as a record of typ for the kernel to
// seal, and reports whether it went: a socket with no room for it takes none
// of it, which reports false and no error.
func sendKernelRecord(fd uintptr, typ byte, data []byte) (bool, error) {
	var space [4]uint64
	oob := unsafe.Slice((*byte)(unsafe.Pointer(&space[0])), syscall.CmsgSpace(1))
	h := (*syscall.Cmsghdr)(unsafe.Pointer(&oob[0]))
	h.Level, h.Type = solTLS, tlsSetRecordType
	h.SetLen(syscall.CmsgLen(1))
	oob[syscall.CmsgLen(0)] = typ
	for {
		n, err := syscall.SendmsgN(int(fd), data, oob, nil, syscall.MSG_NOSIGNAL|syscall.MSG_DONTWAIT)
		switch {
		case err == syscall.EINTR:
			continue
		case err == syscall.EAGAIN:
			return false, nil
		case err != nil:
			return false, err
		case n != len(data):
			return false, syscall.EIO
		}
		return true, nil
	}
}
