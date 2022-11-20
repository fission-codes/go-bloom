package bloom

import (
	"math"

	"github.com/fission-codes/go-bitset"
	"github.com/zeebo/xxh3"
)

type Filter[T any, H HashFunction[T]] struct {
	bitCount  uint64         // filter size in bits
	hashCount uint64         // number of hash functions
	bitSet    *bitset.BitSet // bloom binary
	function  H
}

// NewFilter returns a new Bloom filter with the specified number of bits and hash functions.
// bitCount and hashCount will be set to 1 if a number less than 1 is provided, to avoid panic.
func NewFilter[T any, H HashFunction[T]](bitCount, hashCount uint64, function H) (*Filter[T, H], error) {
	safeBitCount := max(1, bitCount)
	safeHashCount := max(1, hashCount)
	b, err := bitset.New(safeBitCount)
	if err != nil {
		return nil, err
	}
	return &Filter[T, H]{safeBitCount, safeHashCount, b, function}, nil
}

func NewXXH3Filter(bitCount, hashCount uint64) (*Filter[[]byte, HashFunction[[]byte]], error) {
	var function HashFunction[[]byte] = xxh3.HashSeed
	return NewFilter(bitCount, hashCount, function)
}

// NewFilterFromBloomBytes returns a new Bloom filter with the specified number of bits and hash functions,
// and uses bloomBytes as the initial bytes of the Bloom binary.
// bitCount and hashCount will be set to 1 if a number less than 1 is provided, to avoid panic.
func NewFilterFromBloomBytes[T any, H HashFunction[T]](bitCount, hashCount uint64, bloomBytes []byte, function H) *Filter[T, H] {
	safeBitCount := max(1, bitCount)
	safeHashCount := max(1, hashCount)
	return &Filter[T, H]{safeBitCount, safeHashCount, bitset.NewFromBytes(safeBitCount, bloomBytes), function}
}

// Copy returns a pointer to a copy of the filter.
func (f *Filter[T, H]) Copy() *Filter[T, H] {
	return NewFilterFromBloomBytes[T, H](f.bitCount, f.hashCount, f.Bytes(), f.function)
}

// EstimateParameters returns estimates for bitCount and hashCount.
// Calculations are taken from the CAR Mirror spec.
// bitCount will be rounded to the next power of two, as recommended by the spec, to avoid resampling.
func EstimateParameters(n uint64, fpp float64) (bitCount, hashCount uint64) {
	bitCount = NextPowerOfTwo(uint64(math.Ceil(-1 * float64(n) * math.Log(fpp) / math.Pow(math.Log(2), 2))))
	hashCount = uint64(math.Ceil((float64(bitCount) / float64(n)) * math.Log(2)))
	return
}

// NewFilterWithEstimates returns a new Bloom filter with estimated parameters based on the specified
// number of elements and false positive probability rate.
func NewFilterWithEstimates[T any, H HashFunction[T]](n uint64, fpp float64, function H) (*Filter[T, H], error) {
	m, k := EstimateParameters(n, fpp)
	return NewFilter[T, H](m, k, function)
}

func NewXXH3FilterWithEstimates(n uint64, fpp float64) (*Filter[[]byte, HashFunction[[]byte]], error) {
	var function HashFunction[[]byte] = xxh3.HashSeed
	return NewFilterWithEstimates(n, fpp, function)
}

// EstimateFPP returns FPP as one order of magnitude (OOM) under the inverse of the order of magnitude of the number of inserted elements.
// For instance, if there are some 100ks of elements in the filter, then the FPP should be 1/1M.
// TODO: What should we do with 0?
func EstimateFPP(n uint64) float64 {
	if n == 0 {
		return 0.0
	} else {
		return 1 / math.Pow10(int(math.Round(math.Log10(float64(n)))))
	}
}

// BitCount returns the filter size in bits.
func (f *Filter[T, H]) BitCount() uint64 {
	return f.bitCount
}

// HashCount returns the number of hash functions.
func (f *Filter[T, H]) HashCount() uint64 {
	return f.hashCount
}

// Bytes returns the Bloom binary as a byte slice.
func (f *Filter[T, H]) Bytes() []byte {
	return f.bitSet.Bytes()
}

// Add sets hashCount bits of the Bloom filter, using the XXH3 hash with a seed.
// The seed starts at 1 and is incremented by 1 until hashCount bits have been set.
// Any hash that is higher than the bit count is thrown away and the seed is incremented by 1 and we try again.
func (f *Filter[T, H]) Add(data T) *Filter[T, H] {
	hasher := NewHasher[T, H](f.bitCount, f.hashCount, f.function)

	for hasher.Next() {
		nextHash := hasher.Value(data)
		// fmt.Printf("%v\n", nextHash)
		f.bitSet.Set(uint64(nextHash))
	}

	return f
}

// Returns true if all k bits of the Bloom filter are set for the specified data.  Otherwise false.
func (f *Filter[T, H]) Test(data T) bool {
	hasher := NewHasher[T, H](f.bitCount, f.hashCount, f.function)

	for hasher.Next() {
		nextHash := hasher.Value(data)
		if !f.bitSet.Test(uint64(nextHash)) {
			return false
		}
	}

	return true
}

// Union sets this filter's bitset to the union of the other filter's bitset.
func (f *Filter[T, H]) Union(other *Filter[T, H]) {
	f.bitSet.Union(other.bitSet)
}

// Intersect sets this filter's bitset to the intersection of the other filter's bitset.
func (f *Filter[T, H]) Intersect(other *Filter[T, H]) {
	f.bitSet.Intersect(other.bitSet)
}

// FPP returns the false positive probability rate given the number of elements in the filter.
func (f *Filter[T, H]) FPP(n uint64) float64 {
	// Taken from https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions
	return math.Pow(1-math.Pow(math.E, -(((float64(f.BitCount())/float64(n))*math.Log(2))*(float64(n)/float64(f.BitCount())))), (float64(f.BitCount())/float64(n))*math.Log(2))
}

func max(x, y uint64) uint64 {
	if x > y {
		return x
	}
	return y
}

// NextPowerOfTwo returns i if it is a power of 2, otherwise the next power of two greater than i.
func NextPowerOfTwo(i uint64) uint64 {
	i--
	i |= i >> 1
	i |= i >> 2
	i |= i >> 4
	i |= i >> 8
	i |= i >> 16
	i++
	return i
}
