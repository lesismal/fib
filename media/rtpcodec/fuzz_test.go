package rtpcodec

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/sdp"
)

// fuzzFormats are the formats every fuzzed payload goes through.
func fuzzFormats(t testing.TB) []*Format {
	fixtures(t)
	cfg := aac.Config{ObjectType: 2, SampleRate: 48000, ChannelConfig: 2}
	asc, _ := cfg.Append(nil)
	aacTrack := &av.Track{Codec: av.CodecAAC, Config: asc, SampleRate: 48000, Channels: 2}
	generic, _ := NewFormat(aacTrack, 96)
	full := *generic
	full.CTSDeltaLength, full.DTSDeltaLength, full.RandomAccessIndication, full.StreamStateIndication, full.AuxDataSizeLength = 3, 3, true, 1, 4
	latm, _ := NewLATMFormat(aacTrack, 96)
	inband := &Format{PayloadType: 96, ClockRate: 44100, LATM: true, CPresent: true, Track: &av.Track{Codec: av.CodecAAC}}
	h264f, _ := NewFormat(h264Fix.track, 96)
	h265f, _ := NewFormat(h265Fix.track, 96)
	bare264 := &Format{PayloadType: 96, ClockRate: 90000, Track: &av.Track{Codec: av.CodecH264}}
	bare265 := &Format{PayloadType: 96, ClockRate: 90000, Track: &av.Track{Codec: av.CodecH265}}
	opus := &Format{PayloadType: 96, ClockRate: 48000, Track: &av.Track{Codec: av.CodecOpus}}
	return []*Format{h264f, h265f, bare264, bare265, generic, &full, latm, inband, opus}
}

// FuzzDepacketize feeds packets made of the input to a Depacketizer of
// every format. The input is a sequence of [flags][2-byte length][payload]:
// flags carry the marker, a sequence number step and a timestamp step.
func FuzzDepacketize(f *testing.F) {
	for _, name := range []string{"h264", "h265", "aac", "latm", "opus"} {
		var seed []byte
		for i, p := range readPackets(f, name) {
			if i == 12 {
				break
			}
			var q rtp.Packet
			_ = q.Unmarshal(p)
			flags := byte(1)
			if q.Marker {
				flags |= 0x80
			}
			seed = append(seed, flags)
			seed = binary.BigEndian.AppendUint16(seed, uint16(len(q.Payload)))
			seed = append(seed, q.Payload...)
		}
		f.Add(seed)
	}
	formats := fuzzFormats(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, fm := range formats {
			d, err := NewDepacketizer(fm, DepacketizerConfig{MaxAccessUnit: 1 << 16})
			if err != nil {
				t.Fatal(err)
			}
			var seq uint16
			var ts uint32
			b := data
			for len(b) >= 3 {
				flags := b[0]
				n := min(int(binary.BigEndian.Uint16(b[1:])), len(b)-3)
				payload := b[3 : 3+n]
				b = b[3+n:]
				seq += uint16(flags & 3)
				ts += uint32(flags>>2&7) * 1000
				p := rtpPacket(96, seq, ts, flags&0x80 != 0, payload)
				_ = d.Depacketize(p, func(pkt *av.Packet) {
					if pkt.PTS < pkt.DTS && fm.Track.Codec.IsVideo() {
						t.Fatalf("pts %v before dts %v", pkt.PTS, pkt.DTS)
					}
				})
			}
			d.Flush(func(*av.Packet) {})
		}
	})
}

// FuzzPacketize packetizes the input as an access unit of every format
// and depacketizes it again.
func FuzzPacketize(f *testing.F) {
	fixtures(f)
	f.Add(h264Fix.aus[0].Data, true, uint16(200))
	f.Add(h264Fix.aus[1].Data, false, uint16(1400))
	f.Add(h265Fix.aus[0].Data, true, uint16(100))
	f.Add([]byte{1, 2, 3}, true, uint16(30))
	formats := fuzzFormats(f)
	f.Fuzz(func(t *testing.T, data []byte, key bool, mtu uint16) {
		for _, fm := range formats {
			p, err := NewPacketizer(fm, PacketizerConfig{MTU: int(mtu)})
			if err != nil {
				continue
			}
			d, _ := NewDepacketizer(fm, DepacketizerConfig{})
			var q rtp.Packet
			_ = p.Packetize(&av.Packet{Keyframe: key, Data: data}, func(rp *rtp.Packet) error {
				b, err := rp.Append(nil)
				if err != nil {
					return err
				}
				if err := q.Unmarshal(b); err != nil {
					t.Fatal(err)
				}
				return d.Depacketize(&q, func(*av.Packet) {})
			})
		}
	})
}

// FuzzParseMedia parses SDP and makes a Depacketizer and a media section of
// every format it gives.
func FuzzParseMedia(f *testing.F) {
	for _, name := range []string{"h264", "h265", "aac", "latm", "opus", "pcma"} {
		b, err := os.ReadFile(filepath.Join("testdata", name+".sdp"))
		if err != nil {
			f.Fatal(err)
		}
		f.Add(string(b))
	}
	f.Add("v=0\r\no=- 0 0 IN IP4 0.0.0.0\r\ns=-\r\nt=0 0\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\n" +
		"a=fmtp:96 packetization-mode=1;sprop-parameter-sets=Z2QAFazZQUH7ARAAAAMAEAAAAwPI8UKZYA==,aOvssiw=\r\n")
	f.Fuzz(func(t *testing.T, s string) {
		sd, err := sdp.ParseString(s)
		if err != nil {
			return
		}
		for _, fm := range ParseSession(sd) {
			if _, err := NewDepacketizer(fm, DepacketizerConfig{}); err != nil && err != ErrFormat {
				t.Fatal(err)
			}
			if md, err := fm.Media(); err == nil {
				if _, err := ParseMedia(md); err != nil {
					t.Fatalf("%v: %s", err, md.Lines)
				}
			}
			_, _ = NewPacketizer(fm, PacketizerConfig{})
		}
	})
}

// FuzzReorder pushes the sequence numbers of the input through a Reorder
// buffer: what comes out comes out once, in order.
func FuzzReorder(f *testing.F) {
	f.Add([]byte{0, 1, 0, 3, 0, 2, 0, 2, 0, 9, 0xff, 0xff, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReorder(16, 10*time.Millisecond)
		now := time.Unix(0, 0)
		var last uint16
		started := false
		emit := func(p *rtp.Packet) {
			if started && int16(p.SequenceNumber-last) <= 0 && int16(p.SequenceNumber-last) > -100 {
				t.Fatalf("%d after %d", p.SequenceNumber, last)
			}
			started, last = true, p.SequenceNumber
		}
		for len(data) >= 2 {
			seq := binary.BigEndian.Uint16(data)
			data = data[2:]
			now = now.Add(time.Duration(seq&7) * time.Millisecond)
			r.Push(now, rtpPacket(96, seq, 0, false, []byte{1}), emit)
			r.Expire(now, emit)
		}
	})
}

// FuzzStreamMuxConfig reads the input as a StreamMuxConfig and as an
// AudioMuxElement with its config in band.
func FuzzStreamMuxConfig(f *testing.F) {
	f.Add([]byte{0x40, 0x00, 0x24, 0x10, 0x3f, 0xc0})
	f.Add([]byte{0x20, 0x00, 0x12, 0x08, 0x1f, 0xe0, 0x02, 0x01, 0x02})
	f.Fuzz(func(t *testing.T, data []byte) {
		if c, err := parseStreamMuxConfig(data); err == nil {
			if _, err := appendStreamMuxConfig(nil, c.asc); err != nil {
				t.Fatalf("config %x does not write back: %v", c.asc, err)
			}
		}
		var c *streamMuxConfig
		_ = readAudioMuxElement(data, true, &c, func(au []byte) {
			if len(au) > len(data) {
				t.Fatalf("access unit of %d bytes from %d", len(au), len(data))
			}
		})
	})
}
