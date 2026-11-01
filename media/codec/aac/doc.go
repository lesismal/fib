// Package aac handles the two ways AAC streams describe themselves: the
// AudioSpecificConfig that MP4, FLV and RTP carry out of band, and the ADTS
// header MPEG-TS puts before every frame.
//
// Config parses and builds the AudioSpecificConfig of ISO/IEC 14496-3
// section 1.6.2.1, av.Track.Config for AAC: the audio object type,
// escape code 31 included, the sampling frequency by index or as an
// explicit 24-bit value, the channel configuration, and HE-AAC (SBR, PS)
// when signalled explicitly and hierarchically. ADTSHeader parses and
// builds the ADTS frame header of section 1.A.2, with or without its CRC;
// NextADTS splits an ADTS stream into raw access units, the canonical form
// of av.Packet.Data, and Config.AppendADTS writes the header a muxer puts
// back before one.
//
// Left out: program config elements (a channelConfiguration of 0 is
// carried through, its layout unread), the backward-compatible SBR
// signalling after a GASpecificConfig (tolerated, not read), the configs
// of object types other than AAC's beyond their type and rate, ADTS frames
// of several raw data blocks (rejected), checking ADTS CRCs, and LATM.
package aac
