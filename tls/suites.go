package tls

import (
	stdtls "crypto/tls"
	"runtime"
)

// The order this package prefers the TLS 1.3 suites in, as crypto/tls orders
// them: AES-GCM first where the CPU can compute it in hardware, which is faster
// there, and ChaCha20-Poly1305 first where it cannot, where AES-GCM in software
// is slower. crypto/tls also lets a server follow the client's preference among
// them, and peerPrefersAESGCM mirrors that.
var (
	tls13SuitesAES = []uint16{
		stdtls.TLS_AES_128_GCM_SHA256,
		stdtls.TLS_AES_256_GCM_SHA384,
		stdtls.TLS_CHACHA20_POLY1305_SHA256,
	}
	tls13SuitesNoAES = []uint16{
		stdtls.TLS_CHACHA20_POLY1305_SHA256,
		stdtls.TLS_AES_128_GCM_SHA256,
		stdtls.TLS_AES_256_GCM_SHA384,
	}
)

// aesGCMHardware reports whether this is an architecture whose AES-GCM the
// standard library accelerates. crypto/tls probes the instructions at startup,
// through internal/cpu, which this package cannot import: the module has no
// dependencies. It goes by the architecture instead, as crypto/tls's list does
// (amd64, arm64, s390x and ppc64 have the assembly): the older CPUs of those
// architectures that lack the instructions, and GODEBUG=cpu.aes=off, get the
// AES-first order anyway. That chooses speed, not security, and a handshake
// crypto/tls runs itself still makes its own choice. A var so that tests can
// say the CPU has no AES-GCM.
var aesGCMHardware = func() bool {
	switch runtime.GOARCH {
	case "amd64", "arm64", "s390x", "ppc64", "ppc64le":
		return true
	}
	return false
}()

// suites13 is the TLS 1.3 suites in preference order: AES-GCM first where the
// CPU has it and the peer does not put ChaCha20-Poly1305 first; see
// peerPrefersAESGCM.
func suites13(peerPrefersChaCha20 bool) []uint16 {
	if aesGCMHardware && !peerPrefersChaCha20 {
		return tls13SuitesAES
	}
	return tls13SuitesNoAES
}

// peerPrefersAESGCM reports whether suites, a ClientHello's, offers an AES-GCM
// suite before a ChaCha20-Poly1305 one, which is how crypto/tls's
// isAESGCMPreferred picks a server's order: a client's first suite is its
// preference.
func peerPrefersAESGCM(suites []uint16) bool {
	for _, id := range suites {
		switch id {
		case stdtls.TLS_AES_128_GCM_SHA256, stdtls.TLS_AES_256_GCM_SHA384:
			return true
		case stdtls.TLS_CHACHA20_POLY1305_SHA256:
			return false
		}
	}
	return false
}

// isChaCha20Suite reports whether id is one of the ChaCha20-Poly1305 suites the
// record layer protects.
func isChaCha20Suite(id uint16) bool {
	switch id {
	case stdtls.TLS_CHACHA20_POLY1305_SHA256,
		stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
		stdtls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256:
		return true
	}
	return false
}

// chachaFirst is list with its ChaCha20-Poly1305 entries first and the rest
// after them in their order, which is how crypto/tls ranks a TLS 1.2 list where
// AES-GCM is not the cipher to prefer. id reads a suite's ID from an entry.
func chachaFirst[T any](list []T, id func(T) uint16) []T {
	out := make([]T, 0, len(list))
	for _, s := range list {
		if isChaCha20Suite(id(s)) {
			out = append(out, s)
		}
	}
	for _, s := range list {
		if !isChaCha20Suite(id(s)) {
			out = append(out, s)
		}
	}
	return out
}
