// Package mpegts muxes and demuxes MPEG-2 transport streams (ISO/IEC
// 13818-1), the container SRT, HLS and broadcast carry media in, to and from
// the canonical tracks and packets of package av.
//
// Muxer turns av packets into 188-byte transport packets, appended to a
// caller's buffer: a PAT and a PMT (section 2.4.4), repeated at an interval
// and before every video keyframe so a receiver that joins late, or an HLS
// segment that starts at a keyframe, finds them first; one PES (section
// 2.4.3.6) per access unit, or per group of audio frames, with PTS and DTS;
// the PCR on the video PID, or the audio PID of a stream without video,
// with PCR-only packets in between when the PCR stream goes quiet; the
// continuity counters, adaptation-field stuffing, and the
// random_access_indicator on keyframes. H.264 (stream type 0x1B) and H.265
// (0x24) go out in Annex B with an access unit delimiter, and with the
// parameter sets of the track's configuration before every keyframe that
// lacks them; AAC (0x0F) goes out in ADTS; Opus goes out as a private
// stream (0x06) with the 'Opus' registration descriptor, the DVB extension
// descriptor with its channel configuration, and the Opus control header
// before every packet, as the draft ETSI specification of Opus in MPEG-TS
// (opus-codec.org, v0.1.3) has it and ffmpeg implements it. Tracks of other
// codecs are left out.
//
// Demuxer takes a transport stream in chunks of any size, finds the packet
// boundaries again after garbage (resynchronising on 0x47 followed by
// another 0x47 a packet later), follows the PAT and the PMT, PMT updates
// included, reassembles PES packets up to a bound, unwraps the 33-bit PTS
// and DTS, and gives back av tracks and packets: H.264 and H.265 access
// units in AVCC with their configuration records built from the parameter
// sets found in band, AAC frames without ADTS with the AudioSpecificConfig
// its header stands for, and Opus packets with an OpusHead built from the
// descriptor. Tracks are announced once every stream of the program has its
// configuration, or once waiting for one has queued too many packets, and
// again whenever a configuration changes.
//
// Left out: MP3, MPEG-2 video, AC-3 and other stream types (skipped by the
// demuxer, refused by the muxer), multiple programs (the demuxer follows the
// first program of the PAT), PSI sections other than PAT and PMT, scrambled
// packets, 192- and 204-byte packets, and the T-STD buffer model: the muxer
// leads the PCR a fixed time ahead of the DTS rather than modelling decoder
// buffers.
//
// Usage, muxing:
//
//	m, err := mpegts.NewMuxer(tracks, mpegts.MuxerConfig{})
//	buf = m.WriteTables(buf[:0]) // optional: a segment's first bytes
//	buf, err = m.WritePacket(buf, pkt)
//	buf = m.Flush(buf) // grouped audio, at the end of a segment
//
// Usage, demuxing:
//
//	d := mpegts.NewDemuxer(mpegts.DemuxerConfig{
//		OnTracks: func(tracks []*av.Track) { ... },
//		OnPacket: func(pkt *av.Packet) { ... },
//	})
//	d.Write(chunk) // as many times as chunks arrive
//	d.Flush()      // at the end of the stream
package mpegts
