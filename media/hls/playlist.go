package hls

import (
	"strconv"
	"time"
)

// appendSeconds appends d in seconds with three decimals, as EXTINF and
// the part durations have it.
func appendSeconds(b []byte, d time.Duration) []byte {
	return strconv.AppendFloat(b, d.Seconds(), 'f', 3, 64)
}

// MediaPlaylist appends the media playlist (RFC 8216 section 4.4.3, with
// the Low-Latency HLS tags of draft-pantos-hls-rfc8216bis when PartDuration
// is set) to dst, and false when no segment is complete yet: a playlist
// without one would only send players back for another.
//
// EXT-X-TARGETDURATION is TargetDuration rounded up, raised to the rounded
// duration of any segment that came out longer (a keyframe interval longer
// than the target makes them so); it never goes down while the Muxer lives.
func (m *Muxer) MediaPlaylist(dst []byte) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.segs) == 0 {
		return dst, false
	}
	ll := m.config.PartDuration > 0
	b := append(dst, "#EXTM3U\n#EXT-X-VERSION:"...)
	b = strconv.AppendInt(b, int64(m.config.Format.Version), 10)
	b = append(b, "\n#EXT-X-TARGETDURATION:"...)
	b = strconv.AppendInt(b, int64(m.target), 10)
	b = append(b, '\n')
	if ll {
		// PART-HOLD-BACK is three part targets, as the specification
		// recommends; it must be at least two.
		b = append(b, "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK="...)
		b = appendSeconds(b, 3*m.config.PartDuration)
		b = append(b, "\n#EXT-X-PART-INF:PART-TARGET="...)
		b = appendSeconds(b, m.config.PartDuration)
		b = append(b, '\n')
	}
	first := m.first()
	b = append(b, "#EXT-X-MEDIA-SEQUENCE:"...)
	b = strconv.AppendUint(b, m.segs[first].msn, 10)
	b = append(b, '\n')
	disc := m.discRemoved
	for _, s := range m.segs[:first] {
		if s.disc {
			disc++
		}
	}
	if disc > 0 {
		b = append(b, "#EXT-X-DISCONTINUITY-SEQUENCE:"...)
		b = strconv.AppendInt(b, int64(disc), 10)
		b = append(b, '\n')
	}
	// Parts are listed for the segments within three target durations of
	// the end, which is all a player joining at PART-HOLD-BACK needs.
	partsFrom := len(m.segs)
	if ll {
		var tail time.Duration
		for partsFrom > first && tail < 3*time.Duration(m.target)*time.Second {
			partsFrom--
			tail += m.segs[partsFrom].dur
		}
	}
	mapped := -1
	for i, s := range m.segs[first:] {
		b = m.appendSegmentHead(b, s, &mapped)
		if first+i >= partsFrom {
			b = m.appendParts(b, s)
		}
		b = append(b, "#EXTINF:"...)
		b = appendSeconds(b, s.dur)
		b = append(b, ",\n"...)
		b = m.segName(b, s.msn)
		b = append(b, '\n')
	}
	if ll && m.open != nil && len(m.open.parts) > 0 {
		// The open segment's tags go before its first part; without a
		// part yet there is nothing for them to apply to.
		b = m.appendSegmentHead(b, m.open, &mapped)
		b = m.appendParts(b, m.open)
	}
	if m.ended {
		return append(b, "#EXT-X-ENDLIST\n"...), true
	}
	if ll {
		msn, i := m.next, 0
		if m.open != nil {
			msn, i = m.open.msn, len(m.open.parts)
		}
		b = append(b, "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\""...)
		b = m.partName(b, msn, i)
		b = append(b, "\"\n"...)
	}
	return b, true
}

// appendSegmentHead appends the tags before a segment: a discontinuity, and
// EXT-X-MAP when its initialization section is not the one before it.
func (m *Muxer) appendSegmentHead(b []byte, s *segment, mapped *int) []byte {
	if s.disc {
		b = append(b, "#EXT-X-DISCONTINUITY\n"...)
	}
	if s.init != 0 && s.init != *mapped {
		b = append(b, "#EXT-X-MAP:URI=\""...)
		b = m.initName(b, s.init)
		b = append(b, "\"\n"...)
		*mapped = s.init
	}
	return b
}

func (m *Muxer) appendParts(b []byte, s *segment) []byte {
	for i, p := range s.parts {
		b = append(b, "#EXT-X-PART:DURATION="...)
		b = appendSeconds(b, p.dur)
		b = append(b, ",URI=\""...)
		b = m.partName(b, s.msn, i)
		b = append(b, '"')
		if p.independent {
			b = append(b, ",INDEPENDENT=YES"...)
		}
		b = append(b, '\n')
	}
	return b
}

// MultivariantPlaylist appends a multivariant playlist (RFC 8216 section
// 4.4.4) of one variant, the media playlist at mediaURI, to dst, and false
// when no segment is complete yet. BANDWIDTH is the peak bit rate of the
// segments so far, CODECS those of the tracks carried and RESOLUTION the
// video's size, when known.
func (m *Muxer) MultivariantPlaylist(dst []byte, mediaURI string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.segs) == 0 {
		return dst, false
	}
	b := append(dst, "#EXTM3U\n#EXT-X-VERSION:"...)
	b = strconv.AppendInt(b, int64(m.config.Format.Version), 10)
	b = append(b, "\n#EXT-X-STREAM-INF:BANDWIDTH="...)
	b = strconv.AppendInt(b, max(m.peak, 1), 10)
	if m.codecs != "" {
		b = append(b, ",CODECS=\""...)
		b = append(b, m.codecs...)
		b = append(b, '"')
	}
	if m.width > 0 && m.height > 0 {
		b = append(b, ",RESOLUTION="...)
		b = strconv.AppendInt(b, int64(m.width), 10)
		b = append(b, 'x')
		b = strconv.AppendInt(b, int64(m.height), 10)
	}
	b = append(b, '\n')
	b = append(b, mediaURI...)
	return append(b, '\n'), true
}
