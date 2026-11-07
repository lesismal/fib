package bencode

import "errors"

// RawMessage is a bencoded value kept as its bytes: decoding stores the
// value exactly as it was in the input, checking only its syntax, and
// encoding writes it back unchanged. A torrent keeps its info dictionary
// as a RawMessage so that the info hash is taken over the bytes the file
// has.
type RawMessage []byte

// MarshalBencode returns m, which must be one valid value.
func (m RawMessage) MarshalBencode() ([]byte, error) {
	if len(m) == 0 {
		return nil, errors.New("bencode: empty RawMessage")
	}
	return m, nil
}

// UnmarshalBencode sets *m to a copy of data.
func (m *RawMessage) UnmarshalBencode(data []byte) error {
	if m == nil {
		return errors.New("bencode: UnmarshalBencode on nil *RawMessage")
	}
	*m = append((*m)[:0], data...)
	return nil
}
