package kcp

import "errors"

// Reed-Solomon erasure coding over GF(2^8), the field of polynomial 0x11d,
// with the systematic matrix github.com/klauspost/reedsolomon builds by
// default: a Vandermonde matrix times the inverse of its top square. The
// parity shards are then those kcp-go sends, and the shards it recovers
// those kcp-go would.

var (
	gfExp [510]byte
	gfLog [256]byte
	gfMul [256][256]byte
)

func init() {
	x := 1
	for i := 0; i < 255; i++ {
		gfExp[i] = byte(x)
		gfExp[i+255] = byte(x)
		gfLog[x] = byte(i)
		x <<= 1
		if x&0x100 != 0 {
			x ^= 0x11d
		}
	}
	for a := 1; a < 256; a++ {
		for b := 1; b < 256; b++ {
			gfMul[a][b] = gfExp[int(gfLog[a])+int(gfLog[b])]
		}
	}
}

func gfInv(a byte) byte { return gfExp[255-int(gfLog[a])] }

func gfPow(a byte, n int) byte {
	if n == 0 {
		return 1
	}
	if a == 0 {
		return 0
	}
	return gfExp[int(gfLog[a])*n%255]
}

type gfMatrix [][]byte

func newMatrix(rows, cols int) gfMatrix {
	m := make(gfMatrix, rows)
	for r := range m {
		m[r] = make([]byte, cols)
	}
	return m
}

func (m gfMatrix) mul(o gfMatrix) gfMatrix {
	out := newMatrix(len(m), len(o[0]))
	for r := range m {
		for c := range o[0] {
			var v byte
			for i := range o {
				v ^= gfMul[m[r][i]][o[i][c]]
			}
			out[r][c] = v
		}
	}
	return out
}

var errSingular = errors.New("kcp: singular matrix")

// invert returns the inverse of the square matrix m, by Gauss-Jordan
// elimination.
func (m gfMatrix) invert() (gfMatrix, error) {
	n := len(m)
	w := newMatrix(n, 2*n)
	for r := range n {
		copy(w[r], m[r])
		w[r][n+r] = 1
	}
	for r := range n {
		if w[r][r] == 0 {
			for below := r + 1; below < n; below++ {
				if w[below][r] != 0 {
					w[r], w[below] = w[below], w[r]
					break
				}
			}
		}
		if w[r][r] == 0 {
			return nil, errSingular
		}
		if d := w[r][r]; d != 1 {
			inv := gfInv(d)
			for c := range w[r] {
				w[r][c] = gfMul[w[r][c]][inv]
			}
		}
		for o := range n {
			if o == r || w[o][r] == 0 {
				continue
			}
			f := w[o][r]
			for c := range w[o] {
				w[o][c] ^= gfMul[f][w[r][c]]
			}
		}
	}
	out := newMatrix(n, n)
	for r := range n {
		copy(out[r], w[r][n:])
	}
	return out, nil
}

// rsCodec encodes data shards into parity shards and recovers lost data
// shards from any data of them in all.
type rsCodec struct {
	data, parity int
	m            gfMatrix // (data+parity) x data; its top square is the identity
}

func newRS(data, parity int) (*rsCodec, error) {
	if data <= 0 || parity <= 0 || data+parity > 256 {
		return nil, errors.New("kcp: FEC needs 1 to 255 data and parity shards, 256 at most in all")
	}
	total := data + parity
	vm := newMatrix(total, data)
	for r := range total {
		for c := range data {
			vm[r][c] = gfPow(byte(r), c)
		}
	}
	topInv, err := gfMatrix(vm[:data]).invert()
	if err != nil {
		return nil, err
	}
	return &rsCodec{data: data, parity: parity, m: vm.mul(topInv)}, nil
}

func mulAdd(c byte, in, out []byte) {
	if c == 0 {
		return
	}
	if c == 1 {
		for i, v := range in {
			out[i] ^= v
		}
		return
	}
	t := &gfMul[c]
	for i, v := range in {
		out[i] ^= t[v]
	}
}

// encode fills shards[data:] with the parity of shards[:data], all of one
// length.
func (rs *rsCodec) encode(shards [][]byte) {
	for p := range rs.parity {
		out := shards[rs.data+p]
		clear(out)
		row := rs.m[rs.data+p]
		for d := range rs.data {
			mulAdd(row[d], shards[d], out)
		}
	}
}

// reconstructData fills in the nil data shards from the others, given at
// least data shards in all, every one of them size bytes. present says
// which shards are there.
func (rs *rsCodec) reconstructData(shards [][]byte, size int) error {
	rows := make([]int, 0, rs.data)
	for i := range shards {
		if shards[i] != nil {
			rows = append(rows, i)
			if len(rows) == rs.data {
				break
			}
		}
	}
	if len(rows) < rs.data {
		return errors.New("kcp: too few shards to recover from")
	}
	sub := newMatrix(rs.data, rs.data)
	for i, r := range rows {
		copy(sub[i], rs.m[r])
	}
	dec, err := sub.invert()
	if err != nil {
		return err
	}
	for d := range rs.data {
		if shards[d] != nil {
			continue
		}
		out := make([]byte, size)
		for i, r := range rows {
			mulAdd(dec[d][i], shards[r], out)
		}
		shards[d] = out
	}
	return nil
}
