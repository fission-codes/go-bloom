package bloom

import (
	"math"
)

type HashFunction[T any] func(T, uint64) uint64

// Hasher generates hashCount hashes as bit indices for the Bloom filter.
type Hasher[T any, H HashFunction[T]] struct {
	bitCount  uint64 // number of bits we need to index into
	hashCount uint64 // number of hash function calls that result in a bit being set
	seed      uint64 // seed passed into the hash function, which starts at 0
	count     uint64 // number of hash function calls so far
	bitmask   uint64 // used for bitwise-AND to generate an index from the hash
	function  H      // actual hash function used to generate
}

// NewHasher returns a new Hasher
func NewHasher[T any, H HashFunction[T]](bitCount, hashCount uint64, function H) *Hasher[T, H] {
	return &Hasher[T, H]{
		bitCount:  bitCount,
		hashCount: hashCount,
		seed:      0,
		count:     0,
		bitmask:   bitmask(bitCount),
		function:  function,
	}
}

// Next returns true if the Hasher has more hashes to generate.
func (h *Hasher[T, H]) Next() bool {
	return h.count < h.hashCount
}

// Value returns the next hash from the Hasher.
func (h *Hasher[T, H]) Value(data T) uint64 {
	shiftSize := uint64(math.Log2(float64(h.bitmask)))
	var hash, index uint64

	// Attempt to convert hash into a usable index by taking shiftSize right bits and using as an index.
	// If bitCount is not a power of 2, this index may be out of bounds, so cycle through all bits in the
	// 64 bit hash to find an index that is in bounds.  If all bits are exhausted with no viable index,
	// generate a new hash and try again.
	for {
		// Generate hash with current seed
		//hash = xxh3.HashSeed(h.data, h.seed)
		hash = h.function(data, h.seed)
		h.seed += 1

		for i := uint64(0); i < 64; i += shiftSize {
			index = hash & h.bitmask

			// Keep the index if in bounds.
			// If bitCount is a power of 2, we will always break here and thus avoid rejection sampling.
			if index < h.bitCount {
				// We used the hash to generate a valid index.
				// Bump hash count and return the index.
				h.count += 1
				return index
			}

			// index wasn't in bounds, so shift off the used bits and try again
			hash = hash >> shiftSize
			// fmt.Printf("Shifted: shiftSize=%v, bitmask=%v, i=%v, hash=%b\n", shiftSize, h.bitmask, i, hash)
		}
	}
}

// bitmask returns enough right bits set to 1 such that bitwise-AND with the hash will produce an index
// capable of indexing into all bitCount bits
func bitmask(bitCount uint64) uint64 {
	return NextPowerOfTwo(bitCount) - 1
}
