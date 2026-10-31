package srtp

import "strconv"

// Profile is an SRTP protection profile, numbered as DTLS-SRTP's use_srtp
// extension names it (RFC 5764 section 4.1.2, RFC 7714 section 14.2).
type Profile uint16

const (
	ProfileAES128CMHMACSHA1_80 Profile = 0x0001
	ProfileAES128CMHMACSHA1_32 Profile = 0x0002
	ProfileAEADAES128GCM       Profile = 0x0007
	ProfileAEADAES256GCM       Profile = 0x0008
)

// ExporterLabel is the label DTLS-SRTP exports keying material with
// (RFC 5764 section 4.2).
const ExporterLabel = "EXTRACTOR-dtls_srtp"

// Valid tells whether the package implements the profile.
func (p Profile) Valid() bool {
	switch p {
	case ProfileAES128CMHMACSHA1_80, ProfileAES128CMHMACSHA1_32, ProfileAEADAES128GCM, ProfileAEADAES256GCM:
		return true
	}
	return false
}

// KeyLen returns the length of the profile's master key.
func (p Profile) KeyLen() int {
	if p == ProfileAEADAES256GCM {
		return 32
	}
	return 16
}

// SaltLen returns the length of the profile's master salt.
func (p Profile) SaltLen() int {
	if p.aead() {
		return 12
	}
	return 14
}

// KeyingMaterialLen returns how many bytes of keying material to export
// from DTLS for the profile: a key and a salt for each side.
func (p Profile) KeyingMaterialLen() int { return 2 * (p.KeyLen() + p.SaltLen()) }

// RTPOverhead returns how many bytes protecting an RTP packet adds: its
// authentication tag.
func (p Profile) RTPOverhead() int {
	switch p {
	case ProfileAES128CMHMACSHA1_32:
		return 4
	case ProfileAES128CMHMACSHA1_80:
		return 10
	}
	return 16
}

// RTCPOverhead returns how many bytes protecting an RTCP packet adds: its
// SRTCP index and authentication tag. SRTCP keeps the 80-bit tag for
// AES_CM_128_HMAC_SHA1_32 (RFC 5764 section 4.1.2).
func (p Profile) RTCPOverhead() int {
	if p.aead() {
		return 4 + 16
	}
	return 4 + 10
}

func (p Profile) aead() bool { return p == ProfileAEADAES128GCM || p == ProfileAEADAES256GCM }

func (p Profile) String() string {
	switch p {
	case ProfileAES128CMHMACSHA1_80:
		return "SRTP_AES128_CM_HMAC_SHA1_80"
	case ProfileAES128CMHMACSHA1_32:
		return "SRTP_AES128_CM_HMAC_SHA1_32"
	case ProfileAEADAES128GCM:
		return "SRTP_AEAD_AES_128_GCM"
	case ProfileAEADAES256GCM:
		return "SRTP_AEAD_AES_256_GCM"
	}
	return "Profile(" + strconv.Itoa(int(p)) + ")"
}
