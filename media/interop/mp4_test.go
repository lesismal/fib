package interop

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/mp4"
)

// mp4File is a file TestMP4 has ffmpeg write.
type mp4File struct {
	name string
	args []string // the inputs and the encoding options
}

// testInputs are ten seconds of a 320x240 test pattern at 25 fps and a
// tone at rate Hz with channels channels, the tone starting audioDelay
// late (an ffmpeg -itsoffset).
func testInputs(rate, channels int, audioDelay string) []string {
	args := []string{"-f", "lavfi", "-i", "testsrc2=size=320x240:rate=25"}
	if audioDelay != "" {
		args = append(args, "-itsoffset", audioDelay)
	}
	return append(args, "-f", "lavfi", "-i", fmt.Sprintf("sine=frequency=440:sample_rate=%d", rate),
		"-t", "10", "-pix_fmt", "yuv420p", "-ac", strconv.Itoa(channels), "-ar", strconv.Itoa(rate))
}

var (
	x264 = []string{"-c:v", "libx264", "-preset", "veryfast", "-bf", "3", "-g", "50"}
	x265 = []string{"-c:v", "libx265", "-preset", "veryfast", "-tag:v", "hvc1", "-x265-params", "bframes=3:keyint=50:min-keyint=50:log-level=error"}
	aom  = []string{"-c:v", "libaom-av1", "-cpu-used", "8", "-row-mt", "1", "-g", "50", "-b:v", "200k"}
	aac  = []string{"-c:a", "aac", "-b:a", "96k"}
	opus = []string{"-c:a", "libopus", "-b:a", "64k"}
)

func join(parts ...[]string) []string {
	var out []string
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

var mp4Files = []mp4File{
	{"h264-aac-back", join(testInputs(48000, 2, ""), x264, aac)},
	{"h264-aac-front", join(testInputs(44100, 2, ""), x264, aac, []string{"-movflags", "+faststart"})},
	{"h264-opus-negative-cts", join(testInputs(48000, 2, ""), x264, opus, []string{"-movflags", "+faststart+negative_cts_offsets"})},
	{"h265-aac-back", join(testInputs(48000, 1, ""), x265, aac)},
	{"h265-opus-front", join(testInputs(48000, 2, ""), x265, opus, []string{"-movflags", "+faststart"})},
	{"av1-aac-front", join(testInputs(48000, 2, ""), aom, aac, []string{"-movflags", "+faststart"})},
	{"av1-opus-back", join(testInputs(48000, 2, ""), aom, opus)},
	{"h264-aac-delayed-audio", join(testInputs(48000, 2, "0.5"), x264, aac)},
	// Two audio tracks, AAC and Opus, of one tone.
	{"h264-aac-opus", join(testInputs(48000, 2, ""), x264, []string{"-map", "0:v", "-map", "1:a", "-map", "1:a",
		"-c:a:0", "aac", "-c:a:1", "libopus", "-movflags", "+faststart"})},
}

// TestMP4 has ffmpeg write MP4 files of each codec pair, the moov at the
// front and at the back, with B-frames, negative composition offsets,
// audio that starts late and two audio tracks, and checks media/mp4's
// reading of each against ffprobe's: every track's codec, size, rate,
// timescale and configuration, and every sample's offset, size, duration,
// times and sync flag, without the edit list and with it.
func TestMP4(t *testing.T) {
	requireFFmpeg(t)
	dir := t.TempDir()
	encoders := output("ffmpeg", "-hide_banner", "-encoders")
	for _, file := range mp4Files {
		t.Run(file.name, func(t *testing.T) {
			for i, a := range file.args {
				if (strings.HasPrefix(a, "-c:")) && !strings.Contains(encoders, " "+file.args[i+1]+" ") && file.args[i+1] != "aac" {
					t.Skipf("this ffmpeg has no %s", file.args[i+1])
				}
			}
			name := filepath.Join(dir, file.name+".mp4")
			args := append([]string{"-hide_banner", "-v", "error", "-y"}, file.args...)
			if out, err := exec.Command("ffmpeg", append(args, name)...).CombinedOutput(); err != nil {
				t.Fatalf("ffmpeg: %v\n%s", err, out)
			}
			checkMP4(t, name, strings.Contains(file.name, "front") || strings.Contains(strings.Join(file.args, " "), "faststart"))
		})
	}
}

// ffprobeLines runs ffprobe on name with args and returns its compact
// output's lines as maps.
func ffprobeLines(t *testing.T, name string, args ...string) []map[string]string {
	t.Helper()
	cmd := exec.Command("ffprobe", append(append([]string{"-v", "error", "-of", "compact=p=0"}, args...), name)...)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("ffprobe %v: %v", args, err)
	}
	var lines []map[string]string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		m := map[string]string{}
		for _, f := range strings.Split(line, "|") {
			if k, v, ok := strings.Cut(f, "="); ok {
				m[k] = v
			}
		}
		lines = append(lines, m)
	}
	return lines
}

func num(t *testing.T, s string) int64 {
	t.Helper()
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatalf("%q: %v", s, err)
	}
	return v
}

var mp4Codecs = map[string]av.CodecID{"h264": av.CodecH264, "hevc": av.CodecH265, "av1": av.CodecAV1, "aac": av.CodecAAC, "opus": av.CodecOpus}

func checkMP4(t *testing.T, name string, front bool) {
	r, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	st, _ := r.Stat()
	f, err := mp4.Open(r, st.Size())
	if err != nil {
		t.Fatal(err)
	}
	streams := ffprobeLines(t, name, "-show_data_hash", "SHA256", "-show_entries",
		"stream=index,codec_name,width,height,sample_rate,channels,time_base,nb_frames,extradata_hash")
	raw := ffprobeLines(t, name, "-ignore_editlist", "1", "-show_entries", "packet=stream_index,pts,dts,duration,size,pos,flags")
	edited := ffprobeLines(t, name, "-show_entries", "packet=stream_index,pts,dts,pos")
	if len(f.Tracks) != len(streams) {
		t.Fatalf("%d tracks, ffprobe %d", len(f.Tracks), len(streams))
	}
	first := int64(-1)
	for i, tr := range f.Tracks {
		s, c := streams[i], tr.Track
		if c.Codec != mp4Codecs[s["codec_name"]] || "1/"+strconv.Itoa(int(tr.Timescale)) != s["time_base"] || strconv.Itoa(tr.Samples) != s["nb_frames"] ||
			c.Codec.IsVideo() && (strconv.Itoa(c.Width) != s["width"] || strconv.Itoa(c.Height) != s["height"]) ||
			c.Codec.IsAudio() && (strconv.Itoa(c.SampleRate) != s["sample_rate"] || strconv.Itoa(c.Channels) != s["channels"]) {
			t.Fatalf("track %d: %+v timescale %d, %d samples; ffprobe %v", i, c, tr.Timescale, tr.Samples, s)
		}
		if sum := sha256.Sum256(c.Config); "SHA256:"+hex.EncodeToString(sum[:]) != s["extradata_hash"] {
			t.Fatalf("track %d: config %x, ffprobe %s", i, c.Config, s["extradata_hash"])
		}
		tb, err := tr.ReadTable(r)
		if err != nil {
			t.Fatal(err)
		}
		if tb.Bytes() != tr.TableBytes() {
			t.Fatalf("track %d: %d bytes, bound %d", i, tb.Bytes(), tr.TableBytes())
		}
		var want, wantEdited []map[string]string
		for _, p := range raw {
			if p["stream_index"] == strconv.Itoa(i) {
				want = append(want, p)
			}
		}
		for _, p := range edited {
			if p["stream_index"] == strconv.Itoa(i) {
				wantEdited = append(wantEdited, p)
			}
		}
		if len(want) != tb.Len() || len(wantEdited) == 0 {
			t.Fatalf("track %d: %d samples, ffprobe %d and %d", i, tb.Len(), len(want), len(wantEdited))
		}
		// ffmpeg decodes earlier by the least negative composition offset.
		shift := max(0, -int64(tb.MinCTS()))
		byPos := map[int64]mp4.Sample{}
		reordered := false
		for j, smp := range tb.Samples(0) {
			p := want[j]
			if smp.Offset != num(t, p["pos"]) || int64(smp.Size) != num(t, p["size"]) || int64(smp.Duration) != num(t, p["duration"]) ||
				smp.DTS-shift != num(t, p["dts"]) || smp.PTS() != num(t, p["pts"]) || smp.Keyframe != strings.HasPrefix(p["flags"], "K") {
				t.Fatalf("track %d sample %d: %+v, ffprobe %v", i, j, smp, p)
			}
			byPos[smp.Offset] = smp
			reordered = reordered || smp.CTS != tb.Sample(0).CTS
			if first < 0 || smp.Offset < first {
				first = smp.Offset
			}
		}
		if c.Codec == av.CodecH264 || c.Codec == av.CodecH265 {
			if !reordered {
				t.Fatalf("track %d: no B-frames", i)
			}
		}
		for _, p := range wantEdited {
			smp, ok := byPos[num(t, p["pos"])]
			if !ok {
				t.Fatalf("track %d: no sample at %v", i, p)
			}
			if tb.Time(smp.PTS()) != av.FromTicks(num(t, p["pts"]), int(tr.Timescale)) || tb.Time(smp.DTS-shift) != av.FromTicks(num(t, p["dts"]), int(tr.Timescale)) {
				t.Fatalf("track %d: %+v shifted by %d, ffprobe %v", i, smp, tb.Shift, p)
			}
		}
		t.Logf("track %d: %v, %d samples, %d keyframes, edits %+v, table %d bytes", i, c.Codec, tb.Len(), tb.Keyframes(), tr.Edits, tb.Bytes())
	}
	if (f.MoovOffset < first) != front {
		t.Fatalf("moov at %d, the first sample at %d", f.MoovOffset, first)
	}
}
