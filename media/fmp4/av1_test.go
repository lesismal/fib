package fmp4

import (
	"os"
	"testing"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/av1"
)

// TestAV1SizeFromSequenceHeader checks an AV1 track without a size gets
// one in its sample entry from the sequence header of its av1C.
func TestAV1SizeFromSequenceHeader(t *testing.T) {
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
	b, err = AppendInit(nil, []*av.Track{{Codec: av.CodecAV1, Config: config}})
	if err != nil {
		t.Fatal(err)
	}
	init, err := ParseInit(b)
	if err != nil {
		t.Fatal(err)
	}
	if tr := init.Tracks[0].Track; tr.Width != 160 || tr.Height != 120 {
		t.Fatalf("%+v", tr)
	}
}
