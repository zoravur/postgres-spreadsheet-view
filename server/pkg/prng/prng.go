package prng

import (
	"encoding/binary"
	"io"
	"math/rand"
)

// Reader is a deterministic io.Reader backed by a math/rand RNG.
type Reader struct {
	r *rand.Rand
}

// New returns a new deterministic PRNG reader seeded by an integer.
func New(seed int64) io.Reader {
	return &Reader{r: rand.New(rand.NewSource(seed))}
}

// Read fills p with pseudorandom bytes.
func (r *Reader) Read(p []byte) (int, error) {
	n := len(p)
	for i := 0; i < n; i += 8 {
		v := r.r.Int63() // 63-bit random value
		binary.LittleEndian.PutUint64(p[i:], uint64(v))
	}
	return n, nil
}
