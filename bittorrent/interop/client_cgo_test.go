//go:build cgo

package interop

// haveLibutp says anacrolix speaks uTP through go-libutp, the reference
// libutp, which needs cgo; without it anacrolix has a uTP of its own, and
// the uTP cases against anacrolix are skipped.
const haveLibutp = true
