// Package interop checks fib's BitTorrent packages against
// github.com/anacrolix/torrent. It is a module of its own so that
// anacrolix/torrent is never a dependency of github.com/lesismal/fib; its
// tests are in files named after the package they check, such as
// metainfo_test.go.
package interop
