package interop

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/rtc"
)

// The timing of a cell: the publisher sends publishFor of a 25 fps test
// pattern with a tone; startAfter after its stream appears, every player
// starts at once and copies playFor of it while decoding it.
const (
	publishFor = 15 * time.Second
	startAfter = 3 * time.Second
	playFor    = 6 * time.Second
	fps        = 25
	sampleRate = 48000
)

// codecSet is a pair of codecs a source is encoded in.
type codecSet struct {
	name         string
	video, audio string // ffmpeg's codec names, as framecrc reports them
	encode       []string
}

var codecSets = []codecSet{
	{"h264-aac", "h264", "aac", []string{"-c:v", "libx264", "-preset", "veryfast", "-bf", "2", "-c:a", "aac", "-b:a", "96k"}},
	{"h265-aac", "hevc", "aac", []string{"-c:v", "libx265", "-preset", "veryfast", "-x265-params", "bframes=2:keyint=50:min-keyint=50:scenecut=0:log-level=error", "-c:a", "aac", "-b:a", "96k"}},
	{"h264-opus", "h264", "opus", []string{"-c:v", "libx264", "-preset", "veryfast", "-bf", "2", "-c:a", "libopus", "-b:a", "64k"}},
}

// ingest is a protocol ffmpeg publishes over.
type ingest struct {
	name string
	args func(path string) []string // the output options and URL
}

var ingests = []ingest{
	{"rtmp", func(p string) []string { return []string{"-f", "flv", "rtmp://" + RTMPAddr + "/" + p} }},
	{"rtsp-tcp", func(p string) []string {
		return []string{"-f", "rtsp", "-rtsp_transport", "tcp", "rtsp://" + RTSPAddr + "/" + p}
	}},
	{"rtsp-udp", func(p string) []string {
		return []string{"-f", "rtsp", "-rtsp_transport", "udp", "rtsp://" + RTSPAddr + "/" + p}
	}},
	{"srt", func(p string) []string {
		return []string{"-f", "mpegts", "srt://" + SRTAddr + "?streamid=#!::r=" + p + ",m=publish"}
	}},
}

// egress is a protocol ffmpeg plays over. minFrames is the fewest video
// frames playFor may hold: SRT and RTSP over UDP players start at the next
// keyframe, the others at the last one.
type egress struct {
	name      string
	flv       bool // FLV carries it, which ffmpeg reads fewer codecs from
	minFrames int
	args      func(path string) []string // the input options and URL
}

var egresses = []egress{
	{"rtmp", true, 120, func(p string) []string { return []string{"-i", "rtmp://" + RTMPAddr + "/" + p} }},
	{"rtsp-tcp", false, 120, func(p string) []string {
		return []string{"-rtsp_transport", "tcp", "-i", "rtsp://" + RTSPAddr + "/" + p}
	}},
	{"rtsp-udp", false, 90, func(p string) []string {
		return []string{"-rtsp_transport", "udp", "-i", "rtsp://" + RTSPAddr + "/" + p}
	}},
	{"srt", false, 90, func(p string) []string { return []string{"-i", "srt://" + SRTAddr + "?streamid=" + p} }},
	{"http-flv", true, 120, func(p string) []string { return []string{"-i", "http://" + HTTPAddr + "/" + p + ".flv"} }},
	{"hls-fmp4", false, 120, func(p string) []string { return []string{"-i", "http://" + HTTPAddr + "/" + p + "/index.m3u8"} }},
	{"hls-ts", false, 120, func(p string) []string {
		return []string{"-i", "http://" + HTTPAddr + "/" + p + "/index.m3u8?format=ts"}
	}},
	{"ll-hls", false, 120, func(p string) []string { return []string{"-i", "http://" + HTTPAddr + "/ll/" + p + "/index.m3u8"} }},
}

// caps is what the ffmpeg at hand can do.
type caps struct {
	encoders  string
	srt       bool
	flvCodecs map[string]bool // codecs ffmpeg writes into FLV and reads back
}

// requireFFmpeg skips the test when ffmpeg is missing, unless
// FIB_MEDIA_INTEROP is set, as run.sh and CI set it, where it fails.
func requireFFmpeg(t *testing.T) {
	t.Helper()
	for _, name := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(name); err != nil {
			if os.Getenv("FIB_MEDIA_INTEROP") != "" {
				t.Fatalf("%s not found: %v", name, err)
			}
			t.Skipf("%s not found; run.sh runs these checks in a container with it", name)
		}
	}
}

func output(name string, args ...string) string {
	out, _ := exec.Command(name, args...).CombinedOutput()
	return string(out)
}

func probeCaps(t *testing.T, dir string) caps {
	c := caps{
		encoders:  output("ffmpeg", "-hide_banner", "-encoders"),
		srt:       strings.Contains(output("ffmpeg", "-hide_banner", "-protocols"), "srt"),
		flvCodecs: map[string]bool{"h264": true, "aac": true},
	}
	// Enhanced RTMP's H.265 and Opus in FLV: what this ffmpeg writes and
	// reads back, which is what an RTMP or HTTP-FLV player of it reads.
	for _, p := range []struct{ codec, encoder, kind string }{
		{"hevc", "libx265", "v"},
		{"opus", "libopus", "a"},
	} {
		if !c.has(p.encoder) {
			continue
		}
		file := filepath.Join(dir, "probe-"+p.codec+".flv")
		in := []string{"-f", "lavfi", "-i", "testsrc2=size=64x64:rate=25"}
		if p.kind == "a" {
			in = []string{"-f", "lavfi", "-i", "sine=sample_rate=48000"}
		}
		args := append([]string{"-hide_banner", "-v", "error", "-y"}, in...)
		args = append(args, "-t", "0.5", "-c:"+p.kind, p.encoder, "-f", "flv", file)
		if exec.Command("ffmpeg", args...).Run() != nil {
			continue
		}
		got := output("ffprobe", "-v", "error", "-show_entries", "stream=codec_name", "-of", "csv=p=0", file)
		c.flvCodecs[p.codec] = strings.TrimSpace(got) == p.codec
	}
	t.Logf("ffmpeg: %s", strings.SplitN(output("ffmpeg", "-version"), "\n", 2)[0])
	t.Logf("SRT %v, FLV codecs %v", c.srt, c.flvCodecs)
	return c
}

func (c caps) has(encoder string) bool { return strings.Contains(c.encoders, " "+encoder+" ") }

func (c caps) encodes(set codecSet) bool {
	for i, a := range set.encode {
		if (a == "-c:v" || a == "-c:a") && !c.has(set.encode[i+1]) {
			return false
		}
	}
	return true
}

// carries reports whether ffmpeg can read set back over e, or publish it
// over in: FLV, and so RTMP and HTTP-FLV, holds only the codecs this
// ffmpeg writes into it and reads back.
func (c caps) carries(set codecSet, flv bool, name string) bool {
	if name == "srt" && !c.srt {
		return false
	}
	return !flv || c.flvCodecs[set.video] && c.flvCodecs[set.audio]
}

// makeSource encodes publishFor of a 640x360 test pattern with a tone,
// with a two-second GOP, so that publishing it is a copy.
func makeSource(t *testing.T, dir string, set codecSet) string {
	file := filepath.Join(dir, set.name+".mkv")
	args := []string{"-hide_banner", "-v", "error", "-y",
		"-f", "lavfi", "-i", fmt.Sprintf("testsrc2=size=640x360:rate=%d", fps),
		"-f", "lavfi", "-i", fmt.Sprintf("sine=frequency=440:sample_rate=%d", sampleRate),
		"-t", strconv.Itoa(int(publishFor / time.Second)), "-pix_fmt", "yuv420p", "-g", strconv.Itoa(2 * fps),
		"-ac", "2", "-ar", strconv.Itoa(sampleRate)}
	args = append(append(args, set.encode...), file)
	if out, err := exec.Command("ffmpeg", args...).CombinedOutput(); err != nil {
		t.Fatalf("encoding the %s source: %v\n%s", set.name, err, out)
	}
	return file
}

// TestMatrix publishes each codec set over every ingest, the ingests at
// once on paths of their own, and plays each over every egress while it
// is published: ffmpeg must decode playFor with no error lines, the
// codecs, picture size and sample rate must be those published, and DTS
// must increase strictly per video stream and never go back per audio
// stream. fib's WHEP client plays each too, receiving the video and, for
// Opus, the audio. A pair ffmpeg itself cannot publish or read is
// skipped and named in the log.
func TestMatrix(t *testing.T) {
	requireFFmpeg(t)
	dir := t.TempDir()
	c := probeCaps(t, dir)
	s, err := Start()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, set := range codecSets {
		t.Run(set.name, func(t *testing.T) {
			if !c.encodes(set) {
				t.Skipf("this ffmpeg has no encoder for %s", set.name)
			}
			src := makeSource(t, dir, set)
			for _, in := range ingests {
				t.Run(in.name, func(t *testing.T) {
					if !c.carries(set, in.name == "rtmp", in.name) {
						t.Skipf("N/A: this ffmpeg cannot publish %s over %s", set.name, in.name)
					}
					t.Parallel()
					runCell(t, s, c, set, in, src)
				})
			}
		})
	}
}

// result is what one player found.
type result struct {
	summary string
	errs    []string
}

func runCell(t *testing.T, s *Server, c caps, set codecSet, in ingest, src string) {
	path := "live/" + set.name + "-" + in.name
	ctx, cancel := context.WithTimeout(context.Background(), publishFor+time.Minute)
	defer cancel()
	args := append([]string{"-hide_banner", "-nostdin", "-loglevel", "level+warning", "-re", "-i", src, "-map", "0", "-c", "copy"}, in.args(path)...)
	pub := exec.CommandContext(ctx, "ffmpeg", args...)
	var pubLog bytes.Buffer
	pub.Stdout, pub.Stderr = &pubLog, &pubLog
	if err := pub.Start(); err != nil {
		t.Fatal(err)
	}
	pubDone := make(chan error, 1)
	go func() { pubDone <- pub.Wait() }()

	// The players start startAfter the stream is published.
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		if info, ok := s.Hub.Stream(path); ok && info.Publishing && info.PacketsIn > 0 {
			break
		}
		select {
		case err := <-pubDone:
			t.Fatalf("the publisher ended before its stream appeared: %v\n%s", err, tail(pubLog.String()))
		case <-ctx.Done():
			t.Fatalf("the stream never appeared\n%s", tail(pubLog.String()))
		case <-tick.C:
		}
	}
	time.Sleep(startAfter)

	results := make(map[string]result)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, e := range egresses {
		if !c.carries(set, e.flv, e.name) {
			results[e.name] = result{summary: "N/A: this ffmpeg cannot read " + set.name + " over " + e.name}
			continue
		}
		wg.Go(func() {
			r := play(ctx, set, e, path)
			mu.Lock()
			results[e.name] = r
			mu.Unlock()
		})
	}
	wg.Go(func() {
		r := playWHEP(ctx, s, set, path)
		mu.Lock()
		results["whep"] = r
		mu.Unlock()
	})
	wg.Wait()

	pubErr := <-pubDone
	for _, e := range append(egresses, egress{name: "whep"}) {
		r := results[e.name]
		t.Run(e.name, func(t *testing.T) {
			if strings.HasPrefix(r.summary, "N/A") {
				t.Skip(r.summary)
			}
			for _, err := range r.errs {
				t.Error(err)
			}
			t.Log(r.summary)
		})
	}
	if pubErr != nil || len(errorLines(pubLog.String())) != 0 {
		t.Errorf("publisher: %v\n%s", pubErr, tail(pubLog.String()))
	}
}

// play has ffmpeg copy playFor of the stream at path over e into framecrc,
// one line a packet with its DTS, and decode it at the same time. A copy
// waits for a keyframe in each stream, and ffmpeg's RTSP reader flags no
// AAC packet as one, so audio is copied from its first packet.
func play(ctx context.Context, set codecSet, e egress, path string) result {
	args := append([]string{"-hide_banner", "-nostdin", "-loglevel", "level+warning"}, e.args(path)...)
	secs := strconv.Itoa(int(playFor / time.Second))
	args = append(args, "-map", "0", "-c", "copy", "-copyinkf:a", "-t", secs, "-f", "framecrc", "pipe:1",
		"-map", "0", "-t", secs, "-f", "null", "-")
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	var r result
	if err != nil {
		r.errs = append(r.errs, fmt.Sprintf("ffmpeg: %v\n%s", err, tail(stderr.String())))
	}
	for _, l := range errorLines(stderr.String()) {
		r.errs = append(r.errs, "ffmpeg: "+l)
	}
	r.summary, r.errs = checkCRC(stdout.String(), set, e.minFrames, r.errs)
	return r
}

// errorLines returns the error lines of an ffmpeg log at level+warning,
// and the warnings about timestamps going back. ffmpeg's RTSP reader over
// UDP picks its RTP and RTCP ports at random and logs each pair another
// process holds as an error before it tries the next; with many players at
// once that happens, so those lines are left out. A reader that finds no
// pair at all fails, and plays nothing for checkCRC to pass.
func errorLines(log string) []string {
	var out []string
	for _, l := range strings.Split(log, "\n") {
		if strings.Contains(l, "[udp @") && strings.Contains(l, "bind failed: Address already in use") {
			continue
		}
		if strings.Contains(l, "[error]") || strings.Contains(l, "[fatal]") || strings.Contains(l, "[panic]") ||
			strings.Contains(strings.ToLower(l), "monoton") {
			out = append(out, l)
		}
	}
	return out
}

// checkCRC checks a framecrc listing: one video and one audio stream of
// set's codecs, the picture size and sample rate published, at least
// minFrames video frames, and DTS increasing strictly for video and never
// going back for audio.
func checkCRC(crc string, set codecSet, minFrames int, errs []string) (string, []string) {
	type stream struct {
		kind, codec, dims, rate string
		packets                 int
		lastDTS                 int64
		backs                   int
	}
	streams := map[string]*stream{}
	get := func(i string) *stream {
		if streams[i] == nil {
			streams[i] = &stream{lastDTS: -1 << 62}
		}
		return streams[i]
	}
	sc := bufio.NewScanner(strings.NewReader(crc))
	for sc.Scan() {
		l := sc.Text()
		if rest, ok := strings.CutPrefix(l, "#"); ok {
			key, val, _ := strings.Cut(rest, ": ")
			name, idx, _ := strings.Cut(key, " ")
			switch name {
			case "media_type":
				get(idx).kind = val
			case "codec_id":
				get(idx).codec = val
			case "dimensions":
				get(idx).dims = val
			case "sample_rate":
				get(idx).rate = val
			}
			continue
		}
		f := strings.Split(l, ",")
		if len(f) < 3 {
			continue
		}
		st := get(strings.TrimSpace(f[0]))
		dts, err := strconv.ParseInt(strings.TrimSpace(f[1]), 10, 64)
		if err != nil {
			continue
		}
		if dts < st.lastDTS || dts == st.lastDTS && st.kind == "video" {
			st.backs++
		}
		st.lastDTS = dts
		st.packets++
	}
	var v, a *stream
	for _, st := range streams {
		switch st.kind {
		case "video":
			v = st
		case "audio":
			a = st
		}
	}
	if len(streams) != 2 || v == nil || a == nil {
		return "", append(errs, fmt.Sprintf("want one video and one audio stream, got %d streams", len(streams)))
	}
	if v.codec != set.video || v.dims != "640x360" {
		errs = append(errs, fmt.Sprintf("video %s %s, want %s 640x360", v.codec, v.dims, set.video))
	}
	if a.codec != set.audio || a.rate != strconv.Itoa(sampleRate) {
		errs = append(errs, fmt.Sprintf("audio %s at %s Hz, want %s at %d Hz", a.codec, a.rate, set.audio, sampleRate))
	}
	if v.packets < minFrames || v.packets > (int(playFor/time.Second)+1)*fps {
		errs = append(errs, fmt.Sprintf("%d video frames, want %d to %d", v.packets, minFrames, (int(playFor/time.Second)+1)*fps))
	}
	if a.packets < 100 {
		errs = append(errs, fmt.Sprintf("%d audio packets, want 100 or more", a.packets))
	}
	if v.backs != 0 || a.backs != 0 {
		errs = append(errs, fmt.Sprintf("DTS went back or stood still %d times in the video, went back %d times in the audio", v.backs, a.backs))
	}
	return fmt.Sprintf("%s %s %d frames, %s %s Hz %d packets", v.codec, v.dims, v.packets, a.codec, a.rate, a.packets), errs
}

// playWHEP plays the stream at path with fib's WHEP client for playFor:
// it must receive the video, and the audio when it is Opus, since WebRTC
// has no AAC, with DTS never going back.
func playWHEP(ctx context.Context, s *Server, set codecSet, path string) result {
	type track struct {
		codec   av.CodecID
		packets int
		last    time.Duration
		backs   int
	}
	var mu sync.Mutex
	var tracks []*track
	done := make(chan error, 1)
	finish := func(err error) {
		select {
		case done <- err:
		default:
		}
	}
	cl, err := rtc.Pull(rtc.ClientConfig{
		Engine: s.Engine,
		OnTracks: func(ts []*av.Track) {
			mu.Lock()
			defer mu.Unlock()
			tracks = tracks[:0]
			for _, tr := range ts {
				tracks = append(tracks, &track{codec: tr.Codec, last: -1})
			}
		},
		OnPacket: func(p *av.Packet) {
			mu.Lock()
			defer mu.Unlock()
			if p.Track < 0 || p.Track >= len(tracks) {
				return
			}
			tr := tracks[p.Track]
			if p.DTS < tr.last {
				tr.backs++
			}
			tr.last = p.DTS
			tr.packets++
		},
		OnReady: func() { time.AfterFunc(playFor, func() { finish(nil) }) },
		OnClose: finish,
	}, "http://"+HTTPAddr+"/whep/"+path)
	if err != nil {
		return result{errs: []string{"WHEP: " + err.Error()}}
	}
	select {
	case err = <-done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	cl.Close()
	var r result
	if err != nil {
		r.errs = append(r.errs, "WHEP client: "+err.Error())
	}
	mu.Lock()
	defer mu.Unlock()
	want := map[av.CodecID]int{av.CodecH264: 50}
	if set.video == "hevc" {
		want = map[av.CodecID]int{av.CodecH265: 50}
	}
	if set.audio == "opus" {
		want[av.CodecOpus] = 100
	}
	var parts []string
	for _, tr := range tracks {
		parts = append(parts, fmt.Sprintf("%s %d packets", tr.codec, tr.packets))
		if min, ok := want[tr.codec]; ok && tr.packets < min {
			r.errs = append(r.errs, fmt.Sprintf("WHEP: %d %s packets, want %d or more", tr.packets, tr.codec, min))
		}
		if tr.backs != 0 {
			r.errs = append(r.errs, fmt.Sprintf("WHEP: %s DTS went back %d times", tr.codec, tr.backs))
		}
		delete(want, tr.codec)
	}
	for codec := range want {
		r.errs = append(r.errs, fmt.Sprintf("WHEP: no %s track", codec))
	}
	r.summary = strings.Join(parts, ", ")
	return r
}

// tail returns the last lines of an ffmpeg log.
func tail(log string) string {
	lines := strings.Split(strings.TrimRight(log, "\n"), "\n")
	if len(lines) > 20 {
		lines = lines[len(lines)-20:]
	}
	return strings.Join(lines, "\n")
}
