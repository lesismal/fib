package mp4

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/lesismal/fib/media/av"
)

// probe is what testdata/gen.sh had ffprobe say of a file: its streams, and
// for each its packets without the edit list (raw) and with it (pkt).
type probe struct {
	streams  []map[string]string
	raw, pkt [][]map[string]string
}

func readProbe(t testing.TB, name string) *probe {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	p := &probe{}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		fields := strings.Split(sc.Text(), "|")
		m := map[string]string{}
		for _, f := range fields[1:] {
			if k, v, ok := strings.Cut(f, "="); ok {
				m[k] = v
			}
		}
		switch fields[0] {
		case "stream":
			p.streams = append(p.streams, m)
			p.raw = append(p.raw, nil)
			p.pkt = append(p.pkt, nil)
		case "raw", "pkt":
			i := atoi(t, m["stream_index"])
			if fields[0] == "raw" {
				p.raw[i] = append(p.raw[i], m)
			} else {
				p.pkt[i] = append(p.pkt[i], m)
			}
		}
	}
	return p
}

func atoi(t testing.TB, s string) int64 {
	t.Helper()
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatalf("%q: %v", s, err)
	}
	return v
}

// openFile opens a file of testdata.
func openFile(t testing.TB, name string) (*os.File, *File) {
	t.Helper()
	r, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	st, err := r.Stat()
	if err != nil {
		t.Fatal(err)
	}
	f, err := Open(r, st.Size())
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return r, f
}

var codecNames = map[string]av.CodecID{
	"h264": av.CodecH264, "hevc": av.CodecH265, "av1": av.CodecAV1, "aac": av.CodecAAC, "opus": av.CodecOpus,
}

// faststart are the files gen.sh writes with the moov before the media.
var faststart = map[string]bool{"h264_opus": true, "h265_aac": true, "av1_opus": true, "h264_aac_delay": true}

// TestFFprobe reads every file of testdata, ffmpeg's, and checks each
// track and every sample against what ffprobe read: codec, picture size,
// sample rate, channels, timescale, sample count and codec configuration;
// and each sample's offset, size, duration, decode and presentation times
// and sync flag, without the edit list and with it.
func TestFFprobe(t *testing.T) {
	names, _ := filepath.Glob("testdata/*.mp4")
	if len(names) < 7 {
		t.Fatalf("%d files", len(names))
	}
	for _, name := range names {
		base := strings.TrimSuffix(filepath.Base(name), ".mp4")
		t.Run(base, func(t *testing.T) {
			r, f := openFile(t, name)
			p := readProbe(t, strings.TrimSuffix(name, ".mp4")+".probe")
			checkProbe(t, r, f, p)
			// The moov is before the first sample for +faststart, after the
			// last otherwise.
			tb, err := f.Tracks[0].ReadTable(r)
			if err != nil {
				t.Fatal(err)
			}
			if front := f.MoovOffset < tb.Sample(0).Offset; front != faststart[base] {
				t.Fatalf("moov at %d, first sample at %d", f.MoovOffset, tb.Sample(0).Offset)
			}
		})
	}
}

// checkProbe checks f, read through r, against p.
func checkProbe(t *testing.T, r *os.File, f *File, p *probe) {
	t.Helper()
	if len(f.Tracks) != len(p.streams) {
		t.Fatalf("%d tracks, ffprobe %d", len(f.Tracks), len(p.streams))
	}
	for i, tr := range f.Tracks {
		st := p.streams[i]
		c := tr.Track
		if c.Codec != codecNames[st["codec_name"]] || "1/"+strconv.Itoa(int(tr.Timescale)) != st["time_base"] || strconv.Itoa(tr.Samples) != st["nb_frames"] {
			t.Fatalf("track %d: %v timescale %d, %d samples; ffprobe %v", i, c.Codec, tr.Timescale, tr.Samples, st)
		}
		if c.Codec.IsVideo() && (strconv.Itoa(c.Width) != st["width"] || strconv.Itoa(c.Height) != st["height"] || tr.Handler != "vide") ||
			c.Codec.IsAudio() && (strconv.Itoa(c.SampleRate) != st["sample_rate"] || strconv.Itoa(c.Channels) != st["channels"] || tr.Handler != "soun") {
			t.Fatalf("track %d: %+v %q; ffprobe %v", i, c, tr.Handler, st)
		}
		sum := sha256.Sum256(c.Config)
		if "SHA256:"+hex.EncodeToString(sum[:]) != st["extradata_hash"] {
			t.Fatalf("track %d: config %x, ffprobe's hash %s", i, c.Config, st["extradata_hash"])
		}
		if tr.Language != "und" {
			t.Fatalf("track %d: language %q", i, tr.Language)
		}
		tb, err := tr.ReadTable(r)
		if err != nil {
			t.Fatalf("track %d: %v", i, err)
		}
		if tb.Bytes() != tr.TableBytes() || tb.Len() != tr.Samples {
			t.Fatalf("track %d: %d bytes, bound %d", i, tb.Bytes(), tr.TableBytes())
		}
		checkSamples(t, i, tb, p.raw[i], p.pkt[i])
	}
}

func checkSamples(t *testing.T, track int, tb *Table, raw, pkt []map[string]string) {
	t.Helper()
	if len(raw) != tb.Len() {
		t.Fatalf("track %d: %d samples, ffprobe %d", track, tb.Len(), len(raw))
	}
	// ffmpeg's reader decodes earlier by the least negative composition
	// offset, so that no DTS is after its PTS; PTS are the file's.
	dtsShift := max(0, -int64(tb.MinCTS()))
	byPos := map[int64]Sample{}
	var size int64
	for i, s := range tb.Samples(0) {
		e := raw[i]
		if s != tb.Sample(i) {
			t.Fatalf("track %d sample %d: walked %+v, looked up %+v", track, i, s, tb.Sample(i))
		}
		if s.Offset != atoi(t, e["pos"]) || int64(s.Size) != atoi(t, e["size"]) || int64(s.Duration) != atoi(t, e["duration"]) ||
			s.DTS-dtsShift != atoi(t, e["dts"]) || s.PTS() != atoi(t, e["pts"]) || s.Keyframe != strings.HasPrefix(e["flags"], "K") {
			t.Fatalf("track %d sample %d: %+v, ffprobe %v", track, i, s, e)
		}
		byPos[s.Offset] = s
		size += int64(s.Size)
	}
	if size != tb.DataSize() {
		t.Fatalf("track %d: %d bytes of samples, DataSize %d", track, size, tb.DataSize())
	}
	// With the edit list, every packet ffprobe reads is a sample shifted
	// by Shift.
	if len(pkt) == 0 {
		t.Fatalf("track %d: no packets with the edit list", track)
	}
	for _, e := range pkt {
		s, ok := byPos[atoi(t, e["pos"])]
		if !ok {
			t.Fatalf("track %d: no sample at %v", track, e)
		}
		if tb.Time(s.PTS()) != av.FromTicks(atoi(t, e["pts"]), int(tb.Timescale)) ||
			tb.Time(s.DTS-dtsShift) != av.FromTicks(atoi(t, e["dts"]), int(tb.Timescale)) {
			t.Fatalf("track %d: %+v shifted by %d, ffprobe %v", track, s, tb.Shift, e)
		}
	}
}
