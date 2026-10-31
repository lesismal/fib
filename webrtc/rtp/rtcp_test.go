package rtp

import (
	"bytes"
	"errors"
	"math"
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

// TestRTCPErrors checks the error each malformed packet is refused with,
// parsed both as its own type and as part of a compound packet.
func TestRTCPErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    RTCP
		wire string
		err  error
	}{
		{"short header", &SenderReport{}, "80c8", ErrShortPacket},
		{"version", &SenderReport{}, "40c80000", ErrVersion},
		{"SR as RR", &SenderReport{}, "80c90001 00000001", ErrRTCPType},
		{"SR reports past end", &SenderReport{}, "81c80006 00000001 00000000 00000000 00000000 00000000 00000000", ErrRTCPFormat},
		{"RR as SR", &ReceiverReport{}, "80c80001 00000001", ErrRTCPType},
		{"RR no SSRC", &ReceiverReport{}, "80c90000", ErrRTCPFormat},
		{"SDES as BYE", &SourceDescription{}, "80cb0000", ErrRTCPType},
		{"SDES chunk past end", &SourceDescription{}, "81ca0000", ErrRTCPFormat},
		// The END item at offset 4 asks for three bytes of padding the
		// packet's own padding has taken away.
		{"SDES END padding past end", &SourceDescription{}, "a1ca0002 00000001 00000002", ErrRTCPFormat},
		{"BYE as SDES", &Goodbye{}, "80ca0000", ErrRTCPType},
		{"BYE sources past end", &Goodbye{}, "82cb0001 00000001", ErrRTCPFormat},
		{"APP as BYE", &ApplicationDefined{}, "80cb0000", ErrRTCPType},
		{"APP no name", &ApplicationDefined{}, "80cc0001 00000001", ErrRTCPFormat},
		{"padding bit, no body", &PLI{}, "a1ce0000", ErrPadding},
		{"PLI no media SSRC", &PLI{}, "81ce0001 00000001", ErrRTCPFormat},
		{"NACK as TCC", &NACK{}, "8fcd0002 00000001 00000002", ErrRTCPType},
		{"NACK partial pair", &NACK{}, "a1cd0003 00000001 00000002 00640001", ErrRTCPFormat},
		{"FIR as PLI", &FIR{}, "81ce0002 00000001 00000002", ErrRTCPType},
		{"FIR partial entry", &FIR{}, "84ce0003 00000001 00000000 00000002", ErrRTCPFormat},
		{"REMB as FIR", &REMB{}, "84ce0002 00000001 00000000", ErrRTCPType},
		{"TCC as NACK", &TransportCC{}, "81cd0002 00000001 00000002", ErrRTCPType},
		{"TCC no base", &TransportCC{}, "8fcd0003 00000001 00000002 00050001", ErrRTCPFormat},
		{"TCC reserved symbol in a vector", &TransportCC{}, "8fcd0005 00000001 00000002 00050001 00000102 f0000000", ErrRTCPFormat},
		// The packet's padding cuts the large delta's second byte.
		{"TCC large delta past end", &TransportCC{}, "afcd0005 00000001 00000002 00050001 00000102 40010002", ErrRTCPFormat},
		{"raw short", &RawRTCP{}, "80", ErrShortPacket},
		{"raw length", &RawRTCP{}, "80cf0002 00000000", ErrRTCPLength},
	} {
		b := unhex(t, tc.wire)
		if err := tc.p.Unmarshal(b); !errors.Is(err, tc.err) {
			t.Errorf("%s: %v, want %v", tc.name, err, tc.err)
		}
	}
	// A compound packet stops at its first bad packet, returning those
	// before it.
	b := unhex(t, "80c90001 00000007 80c80001 00000001")
	pkts, err := UnmarshalRTCP(b)
	if !errors.Is(err, ErrRTCPFormat) || len(pkts) != 1 || pkts[0].(*ReceiverReport).SSRC != 7 {
		t.Fatalf("%v %v", pkts, err)
	}
	if _, err := UnmarshalRTCP(unhex(t, "40c90000")); !errors.Is(err, ErrVersion) {
		t.Fatal(err)
	}
}

// TestRTCPAppendErrors checks what each type refuses to marshal, and that
// AppendRTCP stops at the first such packet with what it appended before.
func TestRTCPAppendErrors(t *testing.T) {
	long := make([]byte, 256)
	sdesItems := make([]SDESItem, 1100)
	for i := range sdesItems {
		sdesItems[i] = SDESItem{Type: SDESNote, Text: long[:255]}
	}
	for _, tc := range []struct {
		name string
		p    RTCP
		err  error
	}{
		{"SR reports", &SenderReport{Reports: make([]ReceptionReport, 32)}, ErrRTCPTooMany},
		{"SR extensions", &SenderReport{ProfileExtensions: []byte{1, 2, 3}}, ErrRTCPLength},
		{"RR extensions", &ReceiverReport{ProfileExtensions: []byte{1}}, ErrRTCPLength},
		{"SDES chunks", &SourceDescription{Chunks: make([]SDESChunk, 32)}, ErrRTCPTooMany},
		{"SDES too long", &SourceDescription{Chunks: []SDESChunk{{Items: sdesItems}}}, ErrRTCPLength},
		{"SDES END item", &SourceDescription{Chunks: []SDESChunk{{Items: []SDESItem{{Type: SDESEnd}}}}}, ErrRTCPFormat},
		{"BYE sources", &Goodbye{Sources: make([]uint32, 32)}, ErrRTCPTooMany},
		{"BYE reason", &Goodbye{Reason: long}, ErrRTCPItem},
		{"APP subtype", &ApplicationDefined{SubType: 32}, ErrRTCPFormat},
		{"APP data", &ApplicationDefined{Data: []byte{1, 2}}, ErrRTCPLength},
		{"NACK pairs", &NACK{Pairs: make([]NACKPair, 0xffff)}, ErrRTCPTooMany},
		{"FIR entries", &FIR{Entries: make([]FIREntry, 0x8000)}, ErrRTCPTooMany},
		{"REMB SSRCs", &REMB{SSRCs: make([]uint32, 256)}, ErrRTCPTooMany},
		{"TCC status", &TransportCC{Status: make([]uint8, 0x10000)}, ErrRTCPTooMany},
		{"TCC symbol", &TransportCC{Status: []uint8{3}}, ErrRTCPFormat},
		{"TCC missing delta", &TransportCC{Status: []uint8{TCCSmallDelta}}, ErrRTCPFormat},
		{"TCC small delta too large", &TransportCC{Status: []uint8{TCCSmallDelta}, Deltas: []int16{256}}, ErrRTCPFormat},
		{"TCC extra delta", &TransportCC{Status: []uint8{TCCSmallDelta}, Deltas: []int16{1, 2}}, ErrRTCPFormat},
		{"raw body", &RawRTCP{Body: []byte{1}}, ErrRTCPLength},
	} {
		if b, err := tc.p.Append([]byte{9}); !errors.Is(err, tc.err) || !bytes.Equal(b, []byte{9}) {
			t.Errorf("%s: %v, want %v, appended %d bytes", tc.name, err, tc.err, len(b)-1)
		}
	}
	pli := &PLI{SenderSSRC: 1, MediaSSRC: 2}
	b, err := AppendRTCP(nil, pli, &RawRTCP{Body: []byte{1}}, pli)
	if !errors.Is(err, ErrRTCPLength) || len(b) != pli.MarshalSize() {
		t.Fatalf("%d bytes, %v", len(b), err)
	}
}

// TestRTCPEdgeValues round-trips what the sample packets leave out: a
// padded unknown packet, a one-byte BYE reason, which takes padding, and a
// REMB bitrate too large for 64 bits.
func TestRTCPEdgeValues(t *testing.T) {
	b := unhex(t, "a0cf0001 00000001")
	var raw RawRTCP
	if err := raw.Unmarshal(b); err != nil || !raw.Header.Padding || raw.MarshalSize() != len(b) {
		t.Fatalf("%+v %v", raw, err)
	}
	if out, _ := raw.Append(nil); !bytes.Equal(out, b) {
		t.Fatalf("%x", out)
	}

	bye := &Goodbye{Sources: []uint32{1}, Reason: []byte("x")}
	b, _ = bye.Append(nil)
	if !bytes.Equal(b, unhex(t, "81cb0002 00000001 01780000")) {
		t.Fatalf("%x", b)
	}
	var got Goodbye
	if err := got.Unmarshal(b); err != nil || string(got.Reason) != "x" {
		t.Fatal(got, err)
	}

	// Mantissa 3 shifted by 63 overflows; the bitrate saturates.
	var remb REMB
	if err := remb.Unmarshal(unhex(t, "8fce0004 00000001 00000000 52454d42 00fc0003")); err != nil || remb.Bitrate != math.MaxUint64 {
		t.Fatal(remb.Bitrate, err)
	}
	pkts, err := UnmarshalRTCP(unhex(t, "8fce0004 00000001 00000000 52454d42 00fc0003"))
	if err != nil || len(pkts) != 1 {
		t.Fatal(err)
	}
	if _, ok := pkts[0].(*REMB); !ok {
		t.Fatalf("%T", pkts[0])
	}
}

// TestFeedbackIterators stops the iterators early and checks that
// duplicate sequence numbers fold into one NACK bit.
func TestFeedbackIterators(t *testing.T) {
	pair := NACKPair{PacketID: 10, LostPackets: 0b101}
	var got []uint16
	for seq := range pair.All {
		got = append(got, seq)
		break
	}
	for seq := range pair.All {
		got = append(got, seq)
		if len(got) == 3 {
			break
		}
	}
	if !slices.Equal(got, []uint16{10, 10, 11}) {
		t.Fatal(got)
	}
	if pairs := AppendNACKPairs(nil, []uint16{5, 5, 6, 6}); !slices.Equal(pairs, []NACKPair{{5, 1}}) {
		t.Fatal(pairs)
	}
	fb := TransportCC{BaseSeq: 7, Status: []uint8{TCCSmallDelta, TCCNotReceived}, Deltas: []int16{4}}
	n := 0
	for seq, at := range fb.All {
		if seq != 7 || at != 4*TCCDeltaUnit {
			t.Fatal(seq, at)
		}
		n++
		break
	}
	if n != 1 {
		t.Fatal(n)
	}
}

// TestTransportCCRecorderGaps covers feedback with no arrivals, a gap of
// lost packets long enough for a run length chunk, and arrivals too far
// apart in sequence for one packet's 16-bit status count.
func TestTransportCCRecorderGaps(t *testing.T) {
	var r TransportCCRecorder
	if r.Pending() {
		t.Fatal("pending before any arrival")
	}
	if b, err := r.AppendFeedback([]byte{1}); err != nil || len(b) != 1 {
		t.Fatal(b, err)
	}
	r.Record(0, 0)
	r.Record(100, time.Millisecond)
	if !r.Pending() {
		t.Fatal("not pending")
	}
	b, err := r.AppendFeedback(nil)
	if err != nil {
		t.Fatal(err)
	}
	var fb TransportCC
	if err := fb.Unmarshal(b); err != nil {
		t.Fatal(err)
	}
	if len(fb.Status) != 101 || fb.Status[0] != TCCSmallDelta || fb.Status[50] != TCCNotReceived || fb.Status[100] != TCCSmallDelta {
		t.Fatalf("%d symbols", len(fb.Status))
	}
	if r.Pending() {
		t.Fatal("pending after feedback")
	}
	// Each step is under half the sequence space, so the unwrapper keeps
	// counting up: 101, 30101 and 60101 apart, then 90101, past what one
	// packet's status count reaches from the first.
	r.MaxPacketSize = 1 << 20
	for _, seq := range []uint16{101, 30101, 60101, uint16(90101 & 0xffff)} {
		r.Record(seq, 2*time.Millisecond)
	}
	b, err = r.AppendFeedback(nil)
	if err != nil {
		t.Fatal(err)
	}
	pkts, err := UnmarshalRTCP(b)
	if err != nil || len(pkts) != 2 {
		t.Fatal(len(pkts), err)
	}
	if first := pkts[0].(*TransportCC); first.BaseSeq != 101 || len(first.Status) != 60001 {
		t.Fatalf("first covers %d from %d", len(first.Status), first.BaseSeq)
	}
	if second := pkts[1].(*TransportCC); second.BaseSeq != uint16(90101&0xffff) || len(second.Status) != 1 {
		t.Fatalf("second covers %d from %d", len(second.Status), second.BaseSeq)
	}
}
