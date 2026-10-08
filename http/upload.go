//go:build linux || darwin || windows

package http

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"strconv"
	"strings"
)

// PartialSuffix is appended to the path of an upload that is still arriving:
// SaveBody and SaveBodyResumable write to path+PartialSuffix and rename it to
// path once the whole upload is in, so a file at path is always complete.
const PartialSuffix = ".part"

// ErrBadContentRange is what SaveBodyResumable reports for a request whose
// Content-Range is missing, malformed, or does not match the length of its
// body.
var ErrBadContentRange = errors.New("http: missing or malformed Content-Range")

// OffsetError is what SaveBodyResumable reports for a chunk that does not
// start where the stored part of the upload ends. Have is how many bytes are
// stored, which is where the client should continue from.
type OffsetError struct{ Have int64 }

func (e *OffsetError) Error() string {
	return fmt.Sprintf("http: upload chunk does not start at the stored offset %d", e.Have)
}

// Saved describes an upload the Save functions stored, or stopped storing.
type Saved struct {
	// Path is the file the upload is stored in once complete.
	Path string
	// Size is how many bytes of it are stored now: the whole upload when
	// Complete, and for a resumable one the offset to continue from.
	Size int64
	// Total is the size the upload is to reach. For SaveBody it is Size.
	Total int64
	// Complete reports that Path holds the whole upload.
	Complete bool
	// SHA256 is the digest of the whole file, set when Complete.
	SHA256 [sha256.Size]byte
}

// uploadFile is the part of a file the upload code uses.
type uploadFile interface {
	io.ReadWriteCloser
}

// uploadFS is the file system the Save functions work on, a variable so that
// a test can make any one of its calls fail.
var uploadFS = struct {
	open   func(name string, flag int, perm os.FileMode) (uploadFile, error)
	size   func(name string) (int64, error)
	rename func(oldpath, newpath string) error
	remove func(name string) error
}{
	open: func(name string, flag int, perm os.FileMode) (uploadFile, error) {
		file, err := os.OpenFile(name, flag, perm)
		if err != nil {
			return nil, err
		}
		return file, nil
	},
	size: func(name string) (int64, error) {
		info, err := os.Stat(name)
		if err != nil {
			return 0, err
		}
		return info.Size(), nil
	},
	rename: os.Rename,
	remove: os.Remove,
}

// upload is one SaveBody or SaveBodyResumable in progress. Its receive is the
// request's BodyFunc, which the server calls one piece at a time.
type upload struct {
	finish  func(Saved, error)
	file    uploadFile
	path    string
	partial string
	// start is where the first byte of the body lands in the file, written
	// the bytes received so far, and total the size the upload is to reach.
	start, written, total int64
	hash                  interface {
		io.Writer
		Sum([]byte) []byte
	}
	// resumable keeps what has been stored when the body fails, for the
	// client to continue from; a one-shot upload removes it.
	resumable bool
	// over is set once finish has been called, after which what is left of
	// the body is ignored.
	over bool
}

// SaveBody stores the request's body in the file at path, as the body arrives:
// it takes the body through OnBody, writes each piece to path+PartialSuffix
// and renames that to path when the last piece is in, so the handler holds
// none of the body and starts no goroutine of its own. An upload that fails —
// the connection went before the body ended, the body outgrew
// Config.MaxStreamedBodyBytes, a write failed — leaves no file behind, neither
// path nor the partial one.
//
// It needs Config.StreamRequestBody for a body larger than the server is
// willing to hold: with it off the body has arrived whole before the handler
// runs, and SaveBody still stores it, in one piece.
//
// done is called once, with the stored file's description or the error.
// It runs on the connection's worker: from the last body call, or at once if
// the file cannot be created, when nothing is read. It is where the handler
// answers; passing nil answers with RespondSaved. The handler may return as
// soon as SaveBody has.
func (c *Context) SaveBody(path string, done func(Saved, error)) {
	u := &upload{finish: c.uploadDone(done), path: path, partial: path + PartialSuffix, hash: sha256.New()}
	file, err := uploadFS.open(u.partial, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		u.finish(Saved{Path: path}, err)
		return
	}
	u.file = file
	c.OnBody(u.receive)
}

// SaveBodyResumable is SaveBody for an upload sent in several requests, so
// that one cut short by a dropped connection is continued rather than begun
// again. Each request carries one chunk of the file with a Content-Range
// header, "bytes first-last/total", and a body of exactly that length:
//
//	PUT /file   Content-Range: bytes 0-1048575/5000000   (the first MiB)
//	PUT /file   Content-Range: bytes 1048576-2097151/5000000
//	...
//
// The chunks are stored in path+PartialSuffix, which is renamed to path when
// the last byte of total is in. A chunk must start where the stored part
// ends, or at 0, which starts the upload over; otherwise done gets an
// *OffsetError saying where it does end, and the body is not read. A request
// whose body fails midway — the connection went — keeps what arrived of it,
// and done is told the error along with the new Size, so the client asks
// where to continue, with the probe:
//
//	PUT /file   Content-Range: bytes */5000000           (no body)
//
// which done answers, with no error, with the stored Size, or with the whole
// file if it is complete already.
//
// done is called once, as for SaveBody. With Complete set it is the last
// chunk, and SHA256 is the digest of the whole file, read back from disk.
func (c *Context) SaveBodyResumable(path string, done func(Saved, error)) {
	finish := c.uploadDone(done)
	partial := path + PartialSuffix
	start, end, total, probe, ok := parseContentRange(c.Request.Header.Get("Content-Range"))
	if !ok || probe && c.Request.ContentLength != 0 || !probe && c.Request.ContentLength != end-start+1 {
		finish(Saved{Path: path}, ErrBadContentRange)
		return
	}
	have, err := uploadFS.size(partial)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		finish(Saved{Path: path, Total: total}, err)
		return
	}
	saved := Saved{Path: path, Size: have, Total: total}
	if probe {
		if final, err := uploadFS.size(path); err == nil && final == total {
			sum, err := fileSHA256(path)
			finish(Saved{Path: path, Size: total, Total: total, Complete: true, SHA256: sum}, err)
			return
		}
		finish(saved, nil)
		return
	}
	if start != 0 && start != have {
		finish(saved, &OffsetError{Have: have})
		return
	}
	flag := os.O_WRONLY | os.O_CREATE | os.O_APPEND
	if start == 0 {
		flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}
	file, err := uploadFS.open(partial, flag, 0o644)
	if err != nil {
		finish(saved, err)
		return
	}
	u := &upload{finish: finish, file: file, path: path, partial: partial, start: start, total: total, resumable: true}
	c.OnBody(u.receive)
}

// uploadDone is done, or RespondSaved if there is none.
func (c *Context) uploadDone(done func(Saved, error)) func(Saved, error) {
	if done != nil {
		return done
	}
	return func(saved Saved, err error) { _ = c.RespondSaved(saved, err) }
}

// RespondSaved answers an upload the way SaveBody and SaveBodyResumable do
// when they are given no done: 200 with the stored size and digest for a
// complete upload; 202 with the offset to continue from, in Upload-Offset, for
// a chunk of one that is not complete; 409 with Upload-Offset for a chunk in
// the wrong place; 400 for a bad Content-Range; 413 for a body past the
// limit; 500 for anything else. A handler with a done of its own can log, and
// then call this.
func (c *Context) RespondSaved(saved Saved, err error) error {
	header := stdhttp.Header{}
	status, text := stdhttp.StatusOK, ""
	var offset *OffsetError
	switch {
	case errors.As(err, &offset):
		status, text = stdhttp.StatusConflict, err.Error()
		header.Set("Upload-Offset", strconv.FormatInt(offset.Have, 10))
	case errors.Is(err, ErrBadContentRange):
		status, text = stdhttp.StatusBadRequest, err.Error()
	case errors.Is(err, ErrBodyTooLarge):
		status, text = stdhttp.StatusRequestEntityTooLarge, err.Error()
	case err != nil:
		status, text = stdhttp.StatusInternalServerError, err.Error()
	case saved.Complete:
		text = fmt.Sprintf("stored bytes=%d sha256=%x", saved.Size, saved.SHA256)
	default:
		status, text = stdhttp.StatusAccepted, fmt.Sprintf("received bytes=%d of %d", saved.Size, saved.Total)
		header.Set("Upload-Offset", strconv.FormatInt(saved.Size, 10))
	}
	header.Set("Content-Type", "text/plain; charset=utf-8")
	return c.WriteResponse(Response{StatusCode: status, Header: header, Body: []byte(text + "\n")})
}

// receive is the upload's BodyFunc.
func (u *upload) receive(data []byte, fin bool, err error) {
	if u.over {
		return
	}
	if err != nil {
		u.fail(err)
		return
	}
	if len(data) > 0 {
		n, err := u.file.Write(data)
		u.written += int64(n)
		if err != nil {
			u.fail(err)
			return
		}
		if u.hash != nil {
			u.hash.Write(data)
		}
	}
	if !fin {
		return
	}
	if err := u.file.Close(); err != nil {
		u.file = nil
		u.fail(err)
		return
	}
	u.file = nil
	size := u.start + u.written
	saved := Saved{Path: u.path, Size: size, Total: u.total}
	if u.resumable && size < u.total {
		u.stop(saved, nil)
		return
	}
	if err := uploadFS.rename(u.partial, u.path); err != nil {
		u.fail(err)
		return
	}
	saved.Complete = true
	if u.resumable {
		var err error
		if saved.SHA256, err = fileSHA256(u.path); err != nil {
			u.stop(saved, err)
			return
		}
	} else {
		saved.Total = size
		copy(saved.SHA256[:], u.hash.Sum(nil))
	}
	u.stop(saved, nil)
}

// fail ends the upload with err: the partial file is removed, unless the
// upload is resumable, which keeps it for the client to continue.
func (u *upload) fail(err error) {
	if u.file != nil {
		_ = u.file.Close()
		u.file = nil
	}
	if !u.resumable {
		_ = uploadFS.remove(u.partial)
	}
	u.stop(Saved{Path: u.path, Size: u.start + u.written, Total: u.total}, err)
}

func (u *upload) stop(saved Saved, err error) {
	u.over = true
	u.finish(saved, err)
}

// fileSHA256 is the SHA-256 of the file at path.
func fileSHA256(path string) (sum [sha256.Size]byte, err error) {
	file, err := uploadFS.open(path, os.O_RDONLY, 0)
	if err != nil {
		return sum, err
	}
	hash := sha256.New()
	_, err = io.Copy(hash, file)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	copy(sum[:], hash.Sum(nil))
	return sum, err
}

// parseContentRange reads "bytes first-last/total", or the probe form
// "bytes */total".
func parseContentRange(value string) (first, last, total int64, probe, ok bool) {
	spec, found := strings.CutPrefix(value, "bytes ")
	if !found {
		return
	}
	span, size, found := strings.Cut(spec, "/")
	if !found {
		return
	}
	total, err := strconv.ParseInt(size, 10, 64)
	if err != nil || total < 1 {
		return 0, 0, 0, false, false
	}
	if span == "*" {
		return 0, 0, total, true, true
	}
	from, to, found := strings.Cut(span, "-")
	if !found {
		return 0, 0, 0, false, false
	}
	first, err = strconv.ParseInt(from, 10, 64)
	if err != nil || first < 0 {
		return 0, 0, 0, false, false
	}
	last, err = strconv.ParseInt(to, 10, 64)
	if err != nil || last < first || last >= total {
		return 0, 0, 0, false, false
	}
	return first, last, total, false, true
}
