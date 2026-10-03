//go:build linux || darwin || windows

package grpc

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
)

// Codec encodes and decodes messages. Its Name is the content subtype it
// serves: a request with content type application/grpc+json is decoded by
// the Codec named "json", and application/grpc alone by "proto".
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
	Name() string
}

// Compressor compresses messages. Its Name is what grpc-encoding carries.
type Compressor interface {
	Compress(w io.Writer) (io.WriteCloser, error)
	Decompress(r io.Reader) (io.Reader, error)
	Name() string
}

var (
	registryMu  sync.RWMutex
	codecs      = map[string]Codec{}
	compressors = map[string]Compressor{}
)

func init() {
	RegisterCodec(protoCodec{})
	RegisterCodec(jsonCodec{})
	RegisterCompressor(&gzipCompressor{})
}

// RegisterCodec registers c under its lowercased Name, replacing any codec of
// that name. Register one at init time, before any RPC.
//
// The built-in "proto" codec encodes messages that encode themselves:
// those with MarshalVT and UnmarshalVT methods, as vtprotobuf generates, or
// with Marshal and Unmarshal, as gogo/protobuf does. Messages of
// google.golang.org/protobuf, which this module does not depend on, take a
// codec of the program's own:
//
//	type protoCodec struct{}
//
//	func (protoCodec) Name() string                    { return "proto" }
//	func (protoCodec) Marshal(v any) ([]byte, error)    { return proto.Marshal(v.(proto.Message)) }
//	func (protoCodec) Unmarshal(b []byte, v any) error  { return proto.Unmarshal(b, v.(proto.Message)) }
//
//	func init() { grpc.RegisterCodec(protoCodec{}) }
func RegisterCodec(c Codec) {
	if c == nil || c.Name() == "" {
		panic("grpc: RegisterCodec of a nil codec or one with no name")
	}
	registryMu.Lock()
	codecs[strings.ToLower(c.Name())] = c
	registryMu.Unlock()
}

// GetCodec returns the codec registered for name.
func GetCodec(name string) Codec {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return codecs[strings.ToLower(name)]
}

// RegisterCompressor registers c under its Name. The built-in one is gzip.
func RegisterCompressor(c Compressor) {
	if c == nil || c.Name() == "" {
		panic("grpc: RegisterCompressor of a nil compressor or one with no name")
	}
	registryMu.Lock()
	compressors[c.Name()] = c
	registryMu.Unlock()
}

// GetCompressor returns the compressor registered for name.
func GetCompressor(name string) Compressor {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return compressors[name]
}

// acceptEncoding is what grpc-accept-encoding offers: every registered
// compressor.
func acceptEncoding() string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(compressors))
	for name := range compressors {
		names = append(names, name)
	}
	return strings.Join(names, ",")
}

type protoCodec struct{}

func (protoCodec) Name() string { return "proto" }

func (protoCodec) Marshal(v any) ([]byte, error) {
	switch m := v.(type) {
	case interface{ MarshalVT() ([]byte, error) }:
		return m.MarshalVT()
	case interface{ Marshal() ([]byte, error) }:
		return m.Marshal()
	}
	return nil, fmt.Errorf("grpc: the proto codec cannot marshal %T; register a codec of google.golang.org/protobuf with RegisterCodec", v)
}

func (protoCodec) Unmarshal(data []byte, v any) error {
	switch m := v.(type) {
	case interface{ UnmarshalVT([]byte) error }:
		return m.UnmarshalVT(data)
	case interface{ Unmarshal([]byte) error }:
		return m.Unmarshal(data)
	}
	return fmt.Errorf("grpc: the proto codec cannot unmarshal %T; register a codec of google.golang.org/protobuf with RegisterCodec", v)
}

type jsonCodec struct{}

func (jsonCodec) Name() string                       { return "json" }
func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

type gzipCompressor struct {
	writers sync.Pool
	readers sync.Pool
}

func (*gzipCompressor) Name() string { return "gzip" }

func (c *gzipCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	if z, ok := c.writers.Get().(*gzip.Writer); ok {
		z.Reset(w)
		return &pooledGzipWriter{Writer: z, pool: &c.writers}, nil
	}
	return &pooledGzipWriter{Writer: gzip.NewWriter(w), pool: &c.writers}, nil
}

type pooledGzipWriter struct {
	*gzip.Writer
	pool *sync.Pool
}

func (w *pooledGzipWriter) Close() error {
	err := w.Writer.Close()
	w.pool.Put(w.Writer)
	return err
}

func (c *gzipCompressor) Decompress(r io.Reader) (io.Reader, error) {
	z, ok := c.readers.Get().(*gzip.Reader)
	if !ok {
		return gzip.NewReader(r)
	}
	if err := z.Reset(r); err != nil {
		c.readers.Put(z)
		return nil, err
	}
	return &pooledGzipReader{Reader: z, pool: &c.readers}, nil
}

type pooledGzipReader struct {
	*gzip.Reader
	pool *sync.Pool
}

func (r *pooledGzipReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		r.pool.Put(r.Reader)
	}
	return n, err
}

// compress compresses data with c.
func compress(c Compressor, data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := c.Compress(&buf)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decompress decompresses data with c, refusing more than limit bytes.
func decompress(c Compressor, data []byte, limit int) ([]byte, error) {
	r, err := c.Decompress(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	out, err := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(out) > limit {
		return nil, errMessageTooLarge
	}
	return out, nil
}
