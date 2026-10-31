package rtp

import (
	"bytes"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"
)

func samplePackets() []RTCP {
	return []RTCP{
		&SenderReport{SSRC: 1, NTPTime: 0x0102030405060708, RTPTime: 9, PacketCount: 10, OctetCount: 11,
			Reports: []ReceptionReport{{SSRC: 2, FractionLost: 3, TotalLost: -4, LastSequence: 5, Jitter: 6, LastSenderReport: 7, Delay: 8}}},
		&ReceiverReport{SSRC: 1, Reports: []ReceptionReport{{SSRC: 2, TotalLost: 1<<23 - 1}, {SSRC: 3}}, ProfileExtensions: []byte{1, 2, 3, 4}},
		&SourceDescription{Chunks: []SDESChunk{
			{Source: 1, Items: []SDESItem{{Type: SDESCNAME, Text: []byte("cname")}}},
			{Source: 2, Items: []SDESItem{{Type: SDESCNAME, Text: []byte("ab")}, {Type: SDESTool, Text: []byte("fib")}}},
			{Source: 3},
		}},
		&Goodbye{Sources: []uint32{1, 2}, Reason: []byte("bye")},
		&ApplicationDefined{SubType: 3, SSRC: 1, Name: [4]byte{'t', 'e', 's', 't'}, Data: []byte{1, 2, 3, 4}},
		&NACK{SenderSSRC: 1, MediaSSRC: 2, Pairs: []NACKPair{{100, 0x8001}, {200, 0}}},
		&PLI{SenderSSRC: 1, MediaSSRC: 2},
		&FIR{SenderSSRC: 1, Entries: []FIREntry{{SSRC: 2, SequenceNumber: 3}, {SSRC: 4, SequenceNumber: 5}}},
		&REMB{SenderSSRC: 1, Bitrate: 1_500_000 >> 3 << 3, SSRCs: []uint32{2, 3}},
		&TransportCC{SenderSSRC: 1, MediaSSRC: 2, BaseSeq: 65530, ReferenceTime: 0x123456, FeedbackCount: 7,
			Status: []uint8{1, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 1},
			Deltas: []int16{0, 255, -1, 4, 1000, 7}},
	}
}

// TestRTCPRoundTrip marshals a compound packet of every type and parses
// it back to the same values.
func TestRTCPRoundTrip(t *testing.T) {
	pkts := samplePackets()
	b, err := AppendRTCP(nil, pkts...)
	if err != nil {
		t.Fatal(err)
	}
	size := 0
	for _, p := range pkts {
		size += p.MarshalSize()
	}
	if size != len(b) || len(b)%4 != 0 {
		t.Fatalf("size %d, marshaled %d", size, len(b))
	}
	got, err := UnmarshalRTCP(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(pkts) {
		t.Fatalf("%d packets", len(got))
	}
	for i := range pkts {
		want := pkts[i]
		if s, ok := want.(*Goodbye); ok {
			s.Reason = []byte("bye")
		}
		if !reflect.DeepEqual(normalize(got[i]), normalize(want)) {
			t.Errorf("packet %d:\n got %+v\nwant %+v", i, got[i], want)
		}
	}
}

// normalize clears the differences between nil and empty slices.
func normalize(p RTCP) RTCP {
	switch p := p.(type) {
	case *SourceDescription:
		for i := range p.Chunks {
			if len(p.Chunks[i].Items) == 0 {
				p.Chunks[i].Items = nil
			}
		}
	case *ReceiverReport:
		if len(p.ProfileExtensions) == 0 {
			p.ProfileExtensions = nil
		}
	case *SenderReport:
		if len(p.ProfileExtensions) == 0 {
			p.ProfileExtensions = nil
		}
	}
	return p
}

// TestRTCPWire checks packets against bytes laid out by hand from the
// RFCs.
func TestRTCPWire(t *testing.T) {
	for _, tc := range []struct {
		p    RTCP
		wire string
	}{
		{&PLI{SenderSSRC: 0x01020304, MediaSSRC: 0x05060708}, "81ce0002 01020304 05060708"},
		{&NACK{SenderSSRC: 1, MediaSSRC: 2, Pairs: []NACKPair{{0x10, 0x0003}}}, "81cd0003 00000001 00000002 00100003"},
		{&FIR{SenderSSRC: 1, Entries: []FIREntry{{SSRC: 2, SequenceNumber: 9}}}, "84ce0004 00000001 00000000 00000002 09000000"},
		{&REMB{SenderSSRC: 1, Bitrate: 1_000_000, SSRCs: []uint32{5}}, "8fce0005 00000001 00000000 52454d42 010bd090 00000005"},
		{&Goodbye{Sources: []uint32{7}}, "81cb0001 00000007"},
		{&ReceiverReport{SSRC: 9}, "80c90001 00000009"},
		// A CNAME of two bytes, then END and a byte of padding.
		{&SourceDescription{Chunks: []SDESChunk{{Source: 1, Items: []SDESItem{{Type: SDESCNAME, Text: []byte("ab")}}}}},
			"81ca0003 00000001 01026162 00000000"},
		// Two packets received with small deltas, as one run length chunk.
		{&TransportCC{SenderSSRC: 1, MediaSSRC: 2, BaseSeq: 5, ReferenceTime: 1, FeedbackCount: 2,
			Status: []uint8{1, 1}, Deltas: []int16{4, 8}},
			"8fcd0005 00000001 00000002 00050002 00000102 20020408"},
	} {
		want := unhex(t, tc.wire)
		got, err := tc.p.Append(nil)
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("%T: %v\n got %x\nwant %x", tc.p, err, got, want)
			continue
		}
		q := reflect.New(reflect.TypeOf(tc.p).Elem()).Interface().(RTCP)
		if err := q.Unmarshal(want); err != nil {
			t.Errorf("%T: %v", tc.p, err)
		}
	}
}

func TestNACKPairs(t *testing.T) {
	lost := []uint16{65534, 65535, 0, 3, 16, 17, 40, 41}
	pairs := AppendNACKPairs(nil, lost)
	want := []NACKPair{{65534, 1<<0 | 1<<1 | 1<<4}, {16, 1}, {40, 1}}
	if !slices.Equal(pairs, want) {
		t.Fatalf("pairs %v, want %v", pairs, want)
	}
	var got []uint16
	for _, p := range pairs {
		for seq := range p.All {
			got = append(got, seq)
		}
	}
	if !slices.Equal(got, lost) {
		t.Fatalf("seqs %v", got)
	}
}

func TestTransportCCChunks(t *testing.T) {
	for _, status := range [][]uint8{
		{1},
		slices.Repeat([]uint8{0}, 9000),
		slices.Repeat([]uint8{1, 0}, 20),
		slices.Repeat([]uint8{2, 0, 1}, 20),
		append(slices.Repeat([]uint8{1}, 13), 2),
		append(slices.Repeat([]uint8{1}, 30), 0, 2, 0, 1, 1, 1),
	} {
		var deltas []int16
		for _, s := range status {
			switch s {
			case 1:
				deltas = append(deltas, 10)
			case 2:
				deltas = append(deltas, -300)
			}
		}
		p := TransportCC{BaseSeq: 1, Status: status, Deltas: deltas}
		b, err := p.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) != p.MarshalSize() {
			t.Fatalf("size %d, MarshalSize %d", len(b), p.MarshalSize())
		}
		var q TransportCC
		if err := q.Unmarshal(b); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(q.Status, status) || !slices.Equal(q.Deltas, deltas) {
			t.Fatalf("status %v\n got %v", status, q.Status)
		}
	}
	bad := TransportCC{Status: []uint8{1}, Deltas: []int16{256}}
	if _, err := bad.Append(nil); err == nil {
		t.Fatal("small delta of 256 marshaled")
	}
	bad = TransportCC{Status: []uint8{1, 2}, Deltas: []int16{1}}
	if _, err := bad.Append(nil); err == nil {
		t.Fatal("missing delta marshaled")
	}
}

// TestTransportCCRecorder records arrivals, with losses, reordering and a
// sequence number wrap, and reads the feedback back.
func TestTransportCCRecorder(t *testing.T) {
	r := TransportCCRecorder{SenderSSRC: 1, MediaSSRC: 2}
	type arrival struct {
		seq uint16
		at  time.Duration
	}
	in := []arrival{{65533, 100 * time.Millisecond}, {65535, 101 * time.Millisecond}, {65534, 102 * time.Millisecond},
		{1, 103 * time.Millisecond}, {2, 160 * time.Millisecond}, {2, 161 * time.Millisecond}, {3, 150 * time.Millisecond}}
	for _, a := range in {
		r.Record(a.seq, a.at)
	}
	b, err := r.AppendFeedback(nil)
	if err != nil {
		t.Fatal(err)
	}
	pkts, err := UnmarshalRTCP(b)
	if err != nil || len(pkts) != 1 {
		t.Fatal(err, len(pkts))
	}
	fb := pkts[0].(*TransportCC)
	want := map[uint16]time.Duration{65533: 100 * time.Millisecond, 65534: 102 * time.Millisecond, 65535: 101 * time.Millisecond,
		0: -1, 1: 103 * time.Millisecond, 2: 160 * time.Millisecond, 3: 150 * time.Millisecond}
	n := 0
	for seq, at := range fb.All {
		if w, ok := want[seq]; !ok || w != at {
			t.Errorf("seq %d at %v, want %v", seq, at, w)
		}
		n++
	}
	if n != len(want) {
		t.Fatalf("%d packets reported", n)
	}
	// A late packet already reported is dropped; the next feedback starts
	// after the last.
	r.Record(1, 200*time.Millisecond)
	r.Record(4, 10*time.Second)
	b, _ = r.AppendFeedback(b[:0])
	fb = new(TransportCC)
	if err := fb.Unmarshal(b); err != nil {
		t.Fatal(err)
	}
	if fb.BaseSeq != 4 || len(fb.Status) != 1 || fb.FeedbackCount != 1 {
		t.Fatalf("%+v", fb)
	}
	// Arrivals too far apart for one delta split into two packets.
	r.Record(5, 10*time.Second)
	r.Record(6, 20*time.Second)
	b, _ = r.AppendFeedback(b[:0])
	pkts, err = UnmarshalRTCP(b)
	if err != nil || len(pkts) != 2 {
		t.Fatal(err, len(pkts))
	}
	// Many packets split by size.
	r.MaxPacketSize = 200
	for i := range 1000 {
		r.Record(uint16(7+i), 20*time.Second+time.Duration(i)*time.Millisecond)
	}
	b, _ = r.AppendFeedback(b[:0])
	total := 0
	for len(b) > 0 {
		h, pkt, rest, err := NextRTCP(b)
		if err != nil {
			t.Fatal(err)
		}
		if len(pkt) > 200 || h.Type != TypeRTPFB {
			t.Fatalf("packet of %d bytes", len(pkt))
		}
		if err := fb.Unmarshal(pkt); err != nil {
			t.Fatal(err)
		}
		total += len(fb.Status)
		b = rest
	}
	if total != 1000 {
		t.Fatalf("%d packets reported", total)
	}
}

// TestRTCPNoAlloc parses a compound packet into reused values.
func TestRTCPNoAlloc(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates")
	}
	b, err := AppendRTCP(nil, samplePackets()...)
	if err != nil {
		t.Fatal(err)
	}
	var (
		sr   SenderReport
		rr   ReceiverReport
		sdes SourceDescription
		bye  Goodbye
		app  ApplicationDefined
		nack NACK
		pli  PLI
		fir  FIR
		remb REMB
		tcc  TransportCC
	)
	parse := func() {
		for rest := b; len(rest) > 0; {
			h, pkt, next, err := NextRTCP(rest)
			if err != nil {
				t.Fatal(err)
			}
			rest = next
			switch {
			case h.Type == TypeSR:
				err = sr.Unmarshal(pkt)
			case h.Type == TypeRR:
				err = rr.Unmarshal(pkt)
			case h.Type == TypeSDES:
				err = sdes.Unmarshal(pkt)
			case h.Type == TypeBYE:
				err = bye.Unmarshal(pkt)
			case h.Type == TypeAPP:
				err = app.Unmarshal(pkt)
			case h.Type == TypeRTPFB && h.Count == FormatNACK:
				err = nack.Unmarshal(pkt)
			case h.Type == TypeRTPFB && h.Count == FormatTCC:
				err = tcc.Unmarshal(pkt)
			case h.Type == TypePSFB && h.Count == FormatPLI:
				err = pli.Unmarshal(pkt)
			case h.Type == TypePSFB && h.Count == FormatFIR:
				err = fir.Unmarshal(pkt)
			case h.Type == TypePSFB && h.Count == FormatAFB:
				err = remb.Unmarshal(pkt)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	parse()
	if allocs := testing.AllocsPerRun(100, parse); allocs != 0 {
		t.Fatalf("%v allocations", allocs)
	}
}

func TestRTCPMalformed(t *testing.T) {
	b, _ := AppendRTCP(nil, samplePackets()...)
	// Every truncation of a valid compound packet fails or parses a prefix
	// of whole packets.
	for n := range len(b) {
		pkts, err := UnmarshalRTCP(b[:n])
		if err == nil {
			total := 0
			for _, p := range pkts {
				total += p.MarshalSize()
			}
			if total != n {
				t.Fatalf("truncated to %d: parsed %d bytes", n, total)
			}
		}
	}
	for _, tc := range []struct {
		name string
		p    RTCP
		wire string
	}{
		{"SR short", &SenderReport{}, "80c80001 00000001"},
		{"RR count beyond end", &ReceiverReport{}, "81c90001 00000001"},
		{"wrong type", &PLI{}, "81cd0002 00000001 00000002"},
		{"wrong format", &PLI{}, "82ce0002 00000001 00000002"},
		{"length mismatch", &PLI{}, "81ce0003 00000001 00000002"},
		{"padding zero", &PLI{}, "a1ce0002 00000001 00000000"},
		{"padding too long", &PLI{}, "a1ce0002 00000001 000000ff"},
		{"SDES no END", &SourceDescription{}, "81ca0002 00000001 01026162"},
		{"SDES item past end", &SourceDescription{}, "81ca0002 00000001 01096162"},
		{"BYE reason past end", &Goodbye{}, "81cb0002 00000001 05616263"},
		{"REMB no name", &REMB{}, "8fce0004 00000001 00000000 41424344 01000000"},
		{"REMB SSRCs past end", &REMB{}, "8fce0004 00000001 00000000 52454d42 02000000"},
		{"TCC chunks past end", &TransportCC{}, "8fcd0004 00000001 00000002 00050010 00000102"},
		{"TCC deltas past end", &TransportCC{}, "8fcd0005 00000001 00000002 00050003 00000102 20030408"},
		{"TCC reserved symbol", &TransportCC{}, "8fcd0005 00000001 00000002 00050001 00000102 60010000"},
	} {
		if err := tc.p.Unmarshal(unhex(t, tc.wire)); err == nil {
			t.Errorf("%s: parsed", tc.name)
		}
	}
	if _, err := (&ReceiverReport{Reports: make([]ReceptionReport, 32)}).Append(nil); !errors.Is(err, ErrRTCPTooMany) {
		t.Error(err)
	}
	if _, err := (&SourceDescription{Chunks: []SDESChunk{{Items: []SDESItem{{Type: 1, Text: make([]byte, 256)}}}}}).Append(nil); !errors.Is(err, ErrRTCPItem) {
		t.Error(err)
	}
}

func TestRawRTCPAndIsRTCP(t *testing.T) {
	b := unhex(t, "80cf0001 01020304")
	pkts, err := UnmarshalRTCP(b)
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := pkts[0].(*RawRTCP)
	if !ok || raw.Header.Type != 207 {
		t.Fatalf("%T", pkts[0])
	}
	out, _ := raw.Append(nil)
	if !bytes.Equal(out, b) {
		t.Fatalf("%x", out)
	}
	if !IsRTCP(b) || IsRTCP(unhex(t, chromePacket)) {
		t.Fatal("IsRTCP")
	}
}
