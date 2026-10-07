//go:build linux || darwin || windows

package http

import (
	"encoding/json"
	"errors"
	"io"
	"math"
	stdhttp "net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// appendJSON encodes as json.Marshal does, HTML escaping and all, after
// whatever dst holds already.
func TestAppendJSONEncodesAsMarshal(t *testing.T) {
	values := []any{
		nil, 42, "<a href=\"x\">&</a>", []string{"a", "b"},
		map[string]any{"z": 1, "a": []int{1, 2}},
		struct {
			Name string `json:"name"`
			Tags []string
			Skip int `json:"-"`
		}{"fib", nil, 3},
		json.RawMessage(`{"raw" : true}`),
	}
	for _, v := range values {
		want, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		got, err := appendJSON([]byte("prefix:"), v)
		if err != nil || string(got) != "prefix:"+string(want) {
			t.Errorf("appendJSON(%#v) = %q, %v; want %q", v, got, err, "prefix:"+string(want))
		}
	}
	if got, err := appendJSON([]byte("kept"), math.NaN()); err == nil || string(got) != "kept" {
		t.Errorf("appendJSON(NaN) = %q, %v; want dst back and an error", got, err)
	}
}

// JSON answers over HTTP/1 and HTTP/2 alike with the encoding and its type;
// a value it cannot encode sends nothing, leaving the handler to answer.
func TestContextJSON(t *testing.T) {
	type item struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		if r.URL.Path == "/bad" {
			if err := c.JSON(stdhttp.StatusOK, math.Inf(1)); err == nil {
				t.Error("JSON encoded +Inf")
			}
			_ = c.Respond(stdhttp.StatusInternalServerError, "text/plain", []byte("unencodable"))
			return
		}
		n, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/"))
		items := make([]item, n)
		for i := range items {
			items[i] = item{i, strings.Repeat("x", i%50)}
		}
		_ = c.JSON(stdhttp.StatusCreated, items)
	})
	var h1, h2 stdhttp.Protocols
	h1.SetHTTP1(true)
	h2.SetUnencryptedHTTP2(true)
	for name, protocols := range map[string]*stdhttp.Protocols{"HTTP/1.1": &h1, "HTTP/2": &h2} {
		client := &stdhttp.Client{Transport: &stdhttp.Transport{Protocols: protocols}}
		defer client.CloseIdleConnections()
		get := func(path string) (*stdhttp.Response, []byte) {
			t.Helper()
			resp, err := client.Get("http://" + addr + path)
			if err != nil {
				t.Fatalf("%s %s: %v", name, path, err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("%s %s: %v", name, path, err)
			}
			return resp, body
		}
		// Bodies past HTTP/2's initial window wait on flow control, holding
		// the body after JSON returns, while later responses are encoded.
		for _, n := range []int{0, 3, 2000, 5, 2000} {
			resp, body := get("/" + strconv.Itoa(n))
			var items []item
			if err := json.Unmarshal(body, &items); err != nil || len(items) != n {
				t.Fatalf("%s: %d items: %d, %v; body %.80q", name, n, len(items), err, body)
			}
			for i, it := range items {
				if it != (item{i, strings.Repeat("x", i%50)}) {
					t.Fatalf("%s: %d items: item %d = %+v", name, n, i, it)
				}
			}
			if resp.StatusCode != stdhttp.StatusCreated || resp.Header.Get("Content-Type") != "application/json" {
				t.Errorf("%s: %d %q", name, resp.StatusCode, resp.Header.Get("Content-Type"))
			}
		}
		if resp, body := get("/bad"); resp.StatusCode != stdhttp.StatusInternalServerError || string(body) != "unencodable" {
			t.Errorf("%s /bad: %d %q", name, resp.StatusCode, body)
		}
	}
}

// Responses encoded at once on many connections each get their own bytes,
// although HTTP/1 recycles the buffers they are encoded into, with a hook
// that sees the body or without one.
func TestContextJSONConcurrent(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		if strings.HasPrefix(r.URL.Path, "/1") {
			c.OnResponse(func(response *Response) { response.Header.Set("X-Length", strconv.Itoa(len(response.Body))) })
		}
		_ = c.JSON(stdhttp.StatusOK, map[string]string{"path": r.URL.Path, "pad": strings.Repeat(r.URL.Path, 100)})
	})
	client := &stdhttp.Client{Transport: &stdhttp.Transport{MaxIdleConnsPerHost: 16}}
	defer client.CloseIdleConnections()
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				path := "/" + strconv.Itoa(g) + "-" + strconv.Itoa(i)
				resp, err := client.Get("http://" + addr + path)
				if err != nil {
					errs <- err
					return
				}
				var got map[string]string
				err = json.NewDecoder(resp.Body).Decode(&got)
				resp.Body.Close()
				if err != nil || got["path"] != path || got["pad"] != strings.Repeat(path, 100) {
					errs <- errors.New(path + ": answered " + got["path"])
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// JSON encodes into a buffer it recycles where the body is copied out, as
// it is over HTTP/1 with no hook given it, and so allocates less than
// json.Marshal, which copies the encoding into a slice of its own.
func TestContextJSONRecyclesItsBuffer(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops buffers under the race detector")
	}
	type item struct {
		ID   int
		Name string
	}
	items := make([]item, 50)
	for i := range items {
		items[i] = item{i, strings.Repeat("y", 40)}
	}
	marshal := testing.AllocsPerRun(100, func() { _, _ = json.Marshal(items) })
	reused := testing.AllocsPerRun(100, func() {
		buf := jsonBuffers.Get().(*[]byte)
		body, _ := JSONEncoder((*buf)[:0], items)
		*buf = body
		jsonBuffers.Put(buf)
	})
	if reused >= marshal {
		t.Fatalf("encoding into a recycled buffer took %v allocations, json.Marshal %v", reused, marshal)
	}
}

// An HTTP/2 body that waits on flow control after JSON returns is not
// overwritten by the next response JSON encodes.
func TestContextJSONHeldByHTTP2FlowControl(t *testing.T) {
	// One P, so that both streams' handlers take their buffers from the same
	// cache of the pool, and the second would be handed the first's buffer
	// if JSON gave it back while HTTP/2 still held it.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		_ = c.JSON(stdhttp.StatusOK, map[string]string{"v": strings.Repeat(r.URL.Path[1:], 500)})
	})))
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 10})
	first := func(id uint32) {
		t.Helper()
		var got int
		for got < 10 {
			if f := tc.read(); f.typ == h2FrameData && f.streamID == id {
				got += len(f.payload)
			}
		}
	}
	tc.headers(1, true, get("/a")...)
	first(1)
	tc.headers(3, true, get("/b")...)
	first(3)
	tc.write(h2AppendWindowUpdate(nil, 1, 1<<20))
	tc.write(h2AppendWindowUpdate(nil, 3, 1<<20))
	for id, want := range map[uint32]string{1: "a", 3: "b"} {
		_, body := tc.response(id)
		if wantBody := `{"v":"` + strings.Repeat(want, 500) + `"}`; string(body) != wantBody {
			t.Errorf("stream %d: body %.40q..., want %.40q...", id, body, wantBody)
		}
	}
}
