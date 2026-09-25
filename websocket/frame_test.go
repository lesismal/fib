package websocket

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func clientFrame(opcode Opcode, fin bool, payload []byte) []byte {
	first := byte(opcode)
	if fin {
		first |= 0x80
	}
	mask := [4]byte{1, 2, 3, 4}
	header := []byte{first, 0x80}
	if len(payload) < 126 {
		header[1] |= byte(len(payload))
	} else if len(payload) <= 65535 {
		header[1] |= 126
		header = append(header, byte(len(payload)>>8), byte(len(payload)))
	} else {
		header[1] |= 127
		header = append(header, make([]byte, 8)...)
		binary.BigEndian.PutUint64(header[2:10], uint64(len(payload)))
	}
	frame := append(header, mask[:]...)
	for i, value := range payload {
		frame = append(frame, value^mask[i&3])
	}
	return frame
}

func TestParserReleasesOversizedBuffer(t *testing.T) {
	parser := NewParser(1 << 20)
	frame := clientFrame(Binary, true, bytes.Repeat([]byte{'x'}, maxRetainedFrameBuffer+1))
	if _, complete, err := parser.FeedOneBorrowed(frame); err != nil || !complete {
		t.Fatalf("FeedOneBorrowed complete=%v, err=%v", complete, err)
	}
	_, _, _ = parser.FeedOneBorrowed(nil)
	if cap(parser.buffer) > maxRetainedFrameBuffer {
		t.Fatalf("retained buffer capacity = %d", cap(parser.buffer))
	}
}

func TestParserFragmentedMessageAndPing(t *testing.T) {
	parser := NewParser(1024)
	data := append(clientFrame(Text, false, []byte("hel")), clientFrame(Ping, true, []byte("?"))...)
	data = append(data, clientFrame(Continuation, true, []byte("lo"))...)
	var events []Event
	for _, chunk := range [][]byte{data[:3], data[3:9], data[9:]} {
		got, err := parser.Feed(chunk)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, got...)
	}
	if len(events) != 2 || events[0].Opcode != Ping || string(events[0].Payload) != "?" ||
		events[1].Opcode != Text || string(events[1].Payload) != "hello" {
		t.Fatalf("events = %#v", events)
	}
}

func TestParserRejectsProtocolViolations(t *testing.T) {
	t.Run("unmasked", func(t *testing.T) {
		parser := NewParser(100)
		_, err := parser.Feed([]byte{0x81, 0x01, 'x'})
		if !errors.Is(err, ErrProtocol) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("large", func(t *testing.T) {
		parser := NewParser(3)
		_, err := parser.Feed(clientFrame(Binary, true, []byte("four")))
		if !errors.Is(err, ErrMessageTooBig) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("utf8", func(t *testing.T) {
		parser := NewParser(100)
		_, err := parser.Feed(clientFrame(Text, true, []byte{0xff}))
		if !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestMarshalFrameLengths(t *testing.T) {
	for _, size := range []int{0, 125, 126, 65535, 65536} {
		payload := bytes.Repeat([]byte{'x'}, size)
		frame, err := MarshalFrame(Binary, payload)
		if err != nil {
			t.Fatal(err)
		}
		if frame[0] != 0x82 || frame[1]&0x80 != 0 {
			t.Fatalf("invalid header for size %d", size)
		}
		var got uint64
		switch frame[1] {
		case 126:
			got = uint64(binary.BigEndian.Uint16(frame[2:4]))
		case 127:
			got = binary.BigEndian.Uint64(frame[2:10])
		default:
			got = uint64(frame[1])
		}
		if got != uint64(size) {
			t.Fatalf("encoded length = %d, want %d", got, size)
		}
	}
}

func TestFeedOneBorrowedPipelinedFrames(t *testing.T) {
	parser := NewParser(1024)
	data := append(clientFrame(Text, true, []byte("first")), clientFrame(Binary, true, []byte("second"))...)
	first, complete, err := parser.FeedOneBorrowed(data)
	if err != nil || !complete || string(first.Payload) != "first" {
		t.Fatalf("first event = %#v, complete=%v, err=%v", first, complete, err)
	}
	second, complete, err := parser.FeedOneBorrowed(nil)
	if err != nil || !complete || string(second.Payload) != "second" {
		t.Fatalf("second event = %#v, complete=%v, err=%v", second, complete, err)
	}
	parser.ReleaseBorrowed()
	if parser.buffer != nil {
		t.Fatalf("borrowed input retained with capacity %d", cap(parser.buffer))
	}
}

func TestFeedOneBorrowedCompletesOwnedFrameWithoutRetainingTail(t *testing.T) {
	parser := NewParser(4096)
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	first := clientFrame(Binary, true, payload)
	second := clientFrame(Binary, true, []byte("next"))
	cut := 900
	if _, complete, err := parser.FeedOneBorrowed(first[:cut]); err != nil || complete {
		t.Fatalf("partial frame complete=%v, err=%v", complete, err)
	}
	// The adopted buffer must hold the whole frame so the rest of it arrives
	// without a regrow. A recycled buffer may already be larger than that.
	if cap(parser.buffer) < len(first) {
		t.Fatalf("partial buffer capacity=%d, want at least frame size %d", cap(parser.buffer), len(first))
	}
	event, complete, err := parser.FeedOneBorrowed(append(first[cut:], second...))
	if err != nil || !complete || event.Opcode != Binary || len(event.Payload) != len(payload) {
		t.Fatalf("completed first frame: opcode=%d len=%d complete=%v err=%v", event.Opcode, len(event.Payload), complete, err)
	}
	event, complete, err = parser.FeedOneBorrowed(nil)
	if err != nil || !complete || string(event.Payload) != "next" {
		t.Fatalf("borrowed tail: payload=%q complete=%v err=%v", event.Payload, complete, err)
	}
	parser.ReleaseBorrowed()
	if parser.borrowedTail != nil || parser.borrowedBuffer || len(parser.buffer) != 0 {
		t.Fatalf("borrowed input retained after release")
	}
}

// TestFeedOneBorrowedFragmentsAcrossReads feeds a fragmented message in reads
// that end mid-frame, as TCP delivers it. When a read completes a fragment and
// also carries the start of the next frame, those extra bytes must not be lost.
func TestFeedOneBorrowedFragmentsAcrossReads(t *testing.T) {
	message := bytes.Repeat([]byte("*"), 65536)
	for _, fragmentSize := range []int{64, 1300, 4096} {
		var wire []byte
		for offset := 0; offset < len(message); offset += fragmentSize {
			end := min(offset+fragmentSize, len(message))
			opcode := Continuation
			if offset == 0 {
				opcode = Text
			}
			wire = append(wire, clientFrame(opcode, end == len(message), message[offset:end])...)
		}
		for _, readSize := range []int{1, 100, 1300, 4096, len(wire)} {
			parser := NewParser(1 << 20)
			messages := 0
			for offset := 0; offset < len(wire); offset += readSize {
				data := append([]byte(nil), wire[offset:min(offset+readSize, len(wire))]...)
				for {
					event, complete, err := parser.FeedOneBorrowed(data)
					data = nil
					if err != nil {
						t.Fatalf("fragment=%d read=%d offset=%d: %v", fragmentSize, readSize, offset, err)
					}
					if !complete {
						break
					}
					if !bytes.Equal(event.Payload, message) {
						t.Fatalf("fragment=%d read=%d: payload mismatch", fragmentSize, readSize)
					}
					messages++
				}
				parser.ReleaseBorrowed()
			}
			if messages != 1 {
				t.Fatalf("fragment=%d read=%d: got %d messages, want 1", fragmentSize, readSize, messages)
			}
		}
	}
}

// TestPooledBufferNotSharedWhileFrameIsPartial guards the recycling of adopted
// frame buffers. A parser holding half a frame must keep its array until the
// rest arrives; handing that array back while it is still in use would let a
// second connection overwrite the first one's message.
func TestPooledBufferNotSharedWhileFrameIsPartial(t *testing.T) {
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i)
	}
	frame := clientFrame(Binary, true, payload)
	split := len(frame) - 1

	held := NewParser(1 << 20)
	if _, complete, err := held.FeedOneBorrowed(frame[:split]); err != nil || complete {
		t.Fatalf("partial feed: complete=%v err=%v", complete, err)
	}
	held.ReleaseBorrowed()

	// Cycle other parsers through the pool. Each adopts a partial frame of its
	// own, completes it, and releases, so any array the pool is willing to hand
	// out gets filled with a pattern the held frame must not pick up.
	noise := clientFrame(Binary, true, bytes.Repeat([]byte{0xff}, 4096))
	for i := 0; i < 32; i++ {
		other := NewParser(1 << 20)
		if _, complete, err := other.FeedOneBorrowed(noise[:len(noise)-1]); err != nil || complete {
			t.Fatalf("noise partial feed: complete=%v err=%v", complete, err)
		}
		other.ReleaseBorrowed()
		if _, complete, err := other.FeedOneBorrowed(noise[len(noise)-1:]); err != nil || !complete {
			t.Fatalf("noise completion: complete=%v err=%v", complete, err)
		}
		other.ReleaseBorrowed()
	}

	event, complete, err := held.FeedOneBorrowed(frame[split:])
	if err != nil || !complete {
		t.Fatalf("completion: complete=%v err=%v", complete, err)
	}
	if !bytes.Equal(event.Payload, payload) {
		t.Fatal("payload was corrupted while the frame was split across reads")
	}
	held.ReleaseBorrowed()
}

// TestParserPerFrameDeliversEachFrame covers a parser that hands out the
// frames of a fragmented message as they complete: each carries the message's
// own opcode rather than Continuation, only the last has Fin, and a control
// frame between them is unaffected.
func TestParserPerFrameDeliversEachFrame(t *testing.T) {
	parser := NewParser(1024)
	parser.SetPerFrame(true)
	stream := append(clientFrame(Text, false, []byte("hel")), clientFrame(Ping, true, []byte("?"))...)
	stream = append(stream, clientFrame(Continuation, false, []byte("lo "))...)
	stream = append(stream, clientFrame(Continuation, true, []byte("world"))...)
	stream = append(stream, clientFrame(Binary, true, []byte("whole"))...)
	events, err := parser.Feed(stream)
	if err != nil {
		t.Fatal(err)
	}
	want := []Event{
		{Opcode: Text, Payload: []byte("hel")},
		{Opcode: Ping, Payload: []byte("?"), Fin: true},
		{Opcode: Text, Payload: []byte("lo ")},
		{Opcode: Text, Payload: []byte("world"), Fin: true},
		{Opcode: Binary, Payload: []byte("whole"), Fin: true},
	}
	if len(events) != len(want) {
		t.Fatalf("events = %#v, want %d", events, len(want))
	}
	for i, event := range events {
		if event.Opcode != want[i].Opcode || event.Fin != want[i].Fin ||
			!bytes.Equal(event.Payload, want[i].Payload) {
			t.Fatalf("event %d = {opcode %d, %q, fin %v}, want {opcode %d, %q, fin %v}",
				i, event.Opcode, event.Payload, event.Fin, want[i].Opcode, want[i].Payload, want[i].Fin)
		}
	}
	if parser.fragment != nil {
		t.Fatal("a completed message left state behind")
	}
}

// TestParserPerFrameStreamsPastMaxMessageBytes is the point of per-frame
// delivery: nothing accumulates, so a message far larger than the limit goes
// through while the limit still bounds one frame. Reassembling the same
// message fails, which is what a handler taking whole messages wants.
func TestParserPerFrameStreamsPastMaxMessageBytes(t *testing.T) {
	const (
		frameSize = 512
		frames    = 64
	)
	payload := bytes.Repeat([]byte{'x'}, frameSize)
	wire := make([][]byte, frames)
	for i := range wire {
		opcode := Continuation
		if i == 0 {
			opcode = Binary
		}
		wire[i] = clientFrame(opcode, i == frames-1, payload)
	}

	parser := NewParser(frameSize)
	parser.SetPerFrame(true)
	received := 0
	for i, frame := range wire {
		event, complete, err := parser.FeedOneBorrowed(frame)
		if err != nil || !complete {
			t.Fatalf("frame %d: complete=%v err=%v", i, complete, err)
		}
		if event.Opcode != Binary || event.Fin != (i == frames-1) || !bytes.Equal(event.Payload, payload) {
			t.Fatalf("frame %d = {opcode %d, %d bytes, fin %v}", i, event.Opcode, len(event.Payload), event.Fin)
		}
		received += len(event.Payload)
		parser.ReleaseBorrowed()
	}
	if received != frames*frameSize {
		t.Fatalf("received %d bytes, want %d", received, frames*frameSize)
	}
	if parser.fragment != nil || len(parser.buffer) != 0 {
		t.Fatalf("parser held %d bytes of a streamed message", len(parser.buffer))
	}

	whole := NewParser(frameSize)
	var err error
	for _, frame := range wire {
		if _, _, err = whole.FeedOneBorrowed(frame); err != nil {
			break
		}
		whole.ReleaseBorrowed()
	}
	if !errors.Is(err, ErrMessageTooBig) {
		t.Fatalf("reassembling the same message: error = %v, want %v", err, ErrMessageTooBig)
	}
}

// TestParserPerFrameValidatesTextAcrossFrames checks that handing out frames
// as they arrive does not give up on UTF-8: a rune may be split across two
// frames, but the message as a whole still has to be valid text.
func TestParserPerFrameValidatesTextAcrossFrames(t *testing.T) {
	tests := []struct {
		name   string
		frames [][]byte
		want   error
	}{
		{"rune split across frames", [][]byte{{0xce}, {0xba}}, nil},
		{"invalid continuation byte", [][]byte{{0xce}, {0xff}}, ErrInvalidPayload},
		{"message ends mid-rune", [][]byte{{0xce}, {}}, ErrInvalidPayload},
	}
	for _, test := range tests {
		parser := NewParser(1024)
		parser.SetPerFrame(true)
		var stream []byte
		for i, payload := range test.frames {
			opcode := Continuation
			if i == 0 {
				opcode = Text
			}
			stream = append(stream, clientFrame(opcode, i == len(test.frames)-1, payload)...)
		}
		events, err := parser.Feed(stream)
		if !errors.Is(err, test.want) {
			t.Errorf("%s: error = %v, want %v", test.name, err, test.want)
			continue
		}
		if err == nil && (len(events) != len(test.frames) || !events[len(events)-1].Fin) {
			t.Errorf("%s: events = %#v", test.name, events)
		}
	}
}
