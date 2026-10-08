// Package uploadclient is the upload example's client, shared by its HTTP/1,
// HTTP/2 and HTTP/3 versions, which differ only in the Doer they hand it. It
// sends a file to the upload server's three endpoints and checks the SHA-256
// the server reports against its own:
//
//	-mode upload   POST /upload: the whole file in one request
//	-mode resume   PUT /resume: the file in chunks, each with a Content-Range.
//	               It asks the server how much it has first, so a run that was
//	               interrupted (-max-chunks stops one early) continues where
//	               the last left off instead of beginning again
//	-mode echo     POST /echo: the file as a body the server sends straight
//	               back, compared with what was sent
package uploadclient

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Doer sends a request and returns its response: an *http.Client, or anything
// else that speaks HTTP.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// Options are the flags every version of the client takes, and what a run
// works out from the file.
type Options struct {
	server    string
	mode      string
	path      string
	name      string
	chunk     int64
	maxChunks int

	out  io.Writer
	file *os.File
	size int64
	sum  [sha256.Size]byte
}

// Register adds the flags to flags, with server as the default -url.
func Register(flags *flag.FlagSet, server string) *Options {
	o := &Options{}
	flags.StringVar(&o.server, "url", server, "server URL")
	flags.StringVar(&o.mode, "mode", "upload", "upload, resume or echo")
	flags.StringVar(&o.path, "file", "upload-test.bin", "file to send (see ./examples/http/upload/mkfile)")
	flags.StringVar(&o.name, "name", "", "name to store it under, default the file's name")
	flags.Int64Var(&o.chunk, "chunk", 4<<20, "resume: bytes in each request")
	flags.IntVar(&o.maxChunks, "max-chunks", 0, "resume: stop after this many chunks, 0 for all")
	return o
}

// Run does what the flags say, through doer, reporting to out.
func (o *Options) Run(doer Doer, out io.Writer) error {
	o.out = out
	if o.chunk <= 0 {
		return fmt.Errorf("-chunk must be positive, got %d", o.chunk)
	}
	var send func(Doer) error
	switch o.mode {
	case "upload":
		send = o.upload
	case "resume":
		send = o.resume
	case "echo":
		send = o.echo
	default:
		return fmt.Errorf("unknown -mode %q", o.mode)
	}
	if o.name == "" {
		o.name = filepath.Base(o.path)
	}
	file, err := os.Open(o.path)
	if err != nil {
		return err
	}
	defer file.Close()
	// The digest of the file, to check what the server reports against, and
	// its size, as many bytes as were read.
	hash := sha256.New()
	if o.size, err = io.Copy(hash, file); err != nil {
		return err
	}
	o.file = file
	copy(o.sum[:], hash.Sum(nil))
	return send(doer)
}

// endpoint is the server's URL for path, with the name of the file.
func (o *Options) endpoint(path string) (*url.URL, error) {
	target, err := url.Parse(o.server + path)
	if err != nil {
		return nil, err
	}
	query := target.Query()
	query.Set("name", o.name)
	target.RawQuery = query.Encode()
	return target, nil
}

// do sends length bytes of body to path, and returns the response, whose body
// the caller reads and closes.
func (o *Options) do(doer Doer, method, path string, header http.Header, body io.Reader, length int64) (*http.Response, error) {
	target, err := o.endpoint(path)
	if err != nil {
		return nil, err
	}
	req := &http.Request{Method: method, URL: target, Host: target.Host, Header: header, ContentLength: length,
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1}
	if body != nil {
		req.Body = io.NopCloser(body)
	}
	return doer.Do(req)
}

// reply is the whole of resp's body, a short text, and closes it.
func reply(resp *http.Response) (string, error) {
	defer resp.Body.Close()
	text, err := io.ReadAll(resp.Body)
	return string(text), err
}

// verify checks that the server's reply carries the digest of the file.
func (o *Options) verify(proto, text string) error {
	if !strings.Contains(text, fmt.Sprintf("bytes=%d sha256=%x", o.size, o.sum)) {
		return fmt.Errorf("the server's reply does not match the file (%d bytes, sha256 %x): %q", o.size, o.sum, text)
	}
	fmt.Fprintf(o.out, "OK over %s: the server stored exactly what was sent: %s", proto, text)
	return nil
}

var octetStream = http.Header{"Content-Type": {"application/octet-stream"}}

func (o *Options) upload(doer Doer) error {
	resp, err := o.do(doer, http.MethodPost, "/upload", octetStream, io.NewSectionReader(o.file, 0, o.size), o.size)
	if err != nil {
		return err
	}
	text, err := reply(resp)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", resp.Status, text)
	}
	return o.verify(resp.Proto, text)
}

// put sends one chunk, or with a nil body asks how much the server has, and
// returns the server's status, its protocol, its reply, and the offset it
// reports for a reply that carries one.
func (o *Options) put(doer Doer, contentRange string, body io.Reader, length int64) (status int, proto string, offset int64, text string, err error) {
	resp, err := o.do(doer, http.MethodPut, "/resume", http.Header{"Content-Range": {contentRange}}, body, length)
	if err != nil {
		return 0, "", 0, "", err
	}
	if text, err = reply(resp); err != nil {
		return 0, "", 0, "", err
	}
	if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusConflict {
		if offset, err = strconv.ParseInt(resp.Header.Get("Upload-Offset"), 10, 64); err != nil {
			return 0, "", 0, "", fmt.Errorf("no Upload-Offset in the %s reply: %v", resp.Status, err)
		}
	}
	return resp.StatusCode, resp.Proto, offset, text, nil
}

func (o *Options) resume(doer Doer) error {
	// The first request asks where the server's copy ends; each after it
	// sends the next chunk from there.
	status, proto, offset, text, err := o.put(doer, fmt.Sprintf("bytes */%d", o.size), nil, 0)
	for chunks := 0; ; chunks++ {
		if err != nil {
			return err
		}
		switch status {
		case http.StatusOK:
			return o.verify(proto, text)
		case http.StatusAccepted, http.StatusConflict:
			fmt.Fprintf(o.out, "the server has %d of %d bytes\n", offset, o.size)
		default:
			return fmt.Errorf("status %d: %s", status, text)
		}
		if o.maxChunks > 0 && chunks >= o.maxChunks {
			fmt.Fprintln(o.out, "stopping early; run again to continue")
			return nil
		}
		length := min(o.chunk, o.size-offset)
		status, proto, offset, text, err = o.put(doer, fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, o.size),
			io.NewSectionReader(o.file, offset, length), length)
	}
}

// echo sends the file and reads the response as it comes back, hashing it, so
// the file is not held whole on this end either, when the Doer allows.
func (o *Options) echo(doer Doer) error {
	resp, err := o.do(doer, http.MethodPost, "/echo", octetStream, io.NewSectionReader(o.file, 0, o.size), o.size)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		text, _ := reply(resp)
		return fmt.Errorf("%s: %s", resp.Status, text)
	}
	hash := sha256.New()
	n, err := io.Copy(hash, resp.Body)
	if err != nil {
		return err
	}
	if n != o.size || fmt.Sprintf("%x", hash.Sum(nil)) != fmt.Sprintf("%x", o.sum) {
		return fmt.Errorf("the echo differs from the file: %d bytes back of %d", n, o.size)
	}
	fmt.Fprintf(o.out, "OK over %s: the server sent back exactly what was sent: %d bytes\n", resp.Proto, o.size)
	return nil
}
