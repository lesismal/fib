package rtmp

import (
	"os"
	"testing"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/av1"
	"github.com/lesismal/fib/media/flv"
)

// TestIngestAV1Size checks an Enhanced RTMP AV1 sequence start gives the
// ingested track its picture size, from the sequence header.
func TestIngestAV1Size(t *testing.T) {
	b, err := os.ReadFile("../codec/av1/testdata/av1.obu")
	if err != nil {
		t.Fatal(err)
	}
	var config []byte
	for o := range av1.SplitOBUs(b) {
		if o.Type() == av1.OBUSequenceHeader {
			c, err := av1.NewConfig(av1.AppendOBU(nil, o, true))
			if err != nil {
				t.Fatal(err)
			}
			config = c.Append(nil)
			break
		}
	}
	body, err := flv.AppendVideoTag(nil, &flv.VideoTag{FrameType: flv.FrameKey, Codec: av.CodecAV1, PacketType: flv.PacketSequenceStart, Data: config})
	if err != nil {
		t.Fatal(err)
	}
	r := newRecorder(0)
	dec := newIngest(r)
	ingestMessage(dec, &Message{Type: TypeVideo, Payload: body})
	dec.Flush()
	if len(r.tracks) != 1 || len(r.tracks[0]) != 1 {
		t.Fatalf("%+v", r.tracks)
	}
	if tr := r.tracks[0][0]; tr.Codec != av.CodecAV1 || tr.Width != 160 || tr.Height != 120 {
		t.Fatalf("%+v", tr)
	}
}
