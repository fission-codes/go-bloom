package bloom

import (
	"errors"
	"math"
	"reflect"

	"github.com/fission-codes/go-bitset"
	"github.com/zeebo/xxh3"
)

var ERR_INCOMPATIBLE_HASH_FUNCTIONS = errors.New("Incompatible Hash Functions")
var ERR_INCOMPATIBLE_HASH_COUNT = errors.New("Incompatible Hash Count")
var ERR_INCOMPATIBLE_BIT_COUNT = errors.New("Incompatible Bit Count")

type Filter[T any] struct {
	bitCount  uint64         // filter size in bits
	hashCount uint64         // number of hash functions
	bitSet    *bitset.BitSet // bloom binary
	function  HashFunction[T]
}

// NewFilter returns a new Bloom filter with the specified number of bits and hash functions.
// bitCount and hashCount will be set to 1 if a number less than 1 is provided, to avoid panic.
func NewFilter[T any](bitCount, hashCount uint64, function HashFunction[T]) (*Filter[T], error) {
	safeBitCount := max(1, bitCount)
	safeHashCount := max(1, hashCount)
	b, err := bitset.New(safeBitCount)
	if err != nil {
		return nil, err
	}
	return &Filter[T]{safeBitCount, safeHashCount, b, function}, nil
}

func NewXXH3Filter(bitCount, hashCount uint64) (*Filter[[]byte], error) {
	var function HashFunction[[]byte] = xxh3.HashSeed
	return NewFilter(bitCount, hashCount, function)
}

// NewFilterFromBloomBytes returns a new Bloom filter with the specified number of bits and hash functions,
// and uses bloomBytes as the initial bytes of the Bloom binary.
// bitCount and hashCount will be set to 1 if a number less than 1 is provided, to avoid panic.
func NewFilterFromBloomBytes[T any](bitCount, hashCount uint64, bloomBytes []byte, function HashFunction[T]) *Filter[T] {
	safeBitCount := max(1, bitCount)
	safeHashCount := max(1, hashCount)
	return &Filter[T]{safeBitCount, safeHashCount, bitset.NewFromBytes(safeBitCount, bloomBytes), function}
}

// Copy returns a pointer to a copy of the filter.
func (f *Filter[T]) Copy() *Filter[T] {
	return NewFilterFromBloomBytes[T](f.bitCount, f.hashCount, f.Bytes(), f.function)
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
func NewFilterWithEstimates[T any](n uint64, fpp float64, function HashFunction[T]) (*Filter[T], error) {
	m, k := EstimateParameters(n, fpp)
	return NewFilter(m, k, function)
}

func NewXXH3FilterWithEstimates(n uint64, fpp float64) (*Filter[[]byte], error) {
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
func (f *Filter[T]) BitCount() uint64 {
	return f.bitCount
}

// HashCount returns the number of hash functions.
func (f *Filter[T]) HashCount() uint64 {
	return f.hashCount
}

func (f *Filter[T]) EstimateEntries() uint64 {
	bitCount := float64(f.bitCount)
	return uint64(-bitCount*math.Log(1.0-float64(f.bitSet.OnesCount())/bitCount)) / f.hashCount
}

func (f *Filter[T]) EstimateCapacity() uint64 {
	return uint64(float32(f.bitCount) * math.Ln2 / float32(f.hashCount))
}

// Bytes returns the Bloom binary as a byte slice.
func (f *Filter[T]) Bytes() []byte {
	return f.bitSet.Bytes()
}

func (f *Filter[T]) HashFunction() HashFunction[T] {
	return f.function
}

// Add sets hashCount bits of the Bloom filter, using the XXH3 hash with a seed.
// The seed starts at 1 and is incremented by 1 until hashCount bits have been set.
// Any hash that is higher than the bit count is thrown away and the seed is incremented by 1 and we try again.
func (f *Filter[T]) Add(data T) *Filter[T] {
	hasher := NewHasher[T](f.bitCount, f.hashCount, f.function)

	for hasher.Next() {
		nextHash := hasher.Value(data)
		// fmt.Printf("%v\n", nextHash)
		f.bitSet.Set(uint64(nextHash))
	}

	return f
}

// Returns true if all k bits of the Bloom filter are set for the specified data.  Otherwise false.
func (f *Filter[T]) Test(data T) bool {
	hasher := NewHasher[T](f.bitCount, f.hashCount, f.function)

	for hasher.Next() {
		nextHash := hasher.Value(data)
		if !f.bitSet.Test(uint64(nextHash)) {
			return false
		}
	}

	return true
}

func (f *Filter[T]) checkCompatibility(other *Filter[T]) error {
	if reflect.ValueOf(f.function).Pointer() != reflect.ValueOf(other.function).Pointer() {
		return ERR_INCOMPATIBLE_HASH_FUNCTIONS
	}
	if f.hashCount != other.hashCount {
		return ERR_INCOMPATIBLE_HASH_COUNT
	}
	if f.bitCount != other.bitCount {
		return ERR_INCOMPATIBLE_BIT_COUNT
	}
	return nil
}

// Union sets this filter's bitset to the union of the other filter's bitset.
func (f *Filter[T]) Union(other *Filter[T]) error {
	error := f.checkCompatibility(other)
	if error == nil {
		error = f.bitSet.Union(other.bitSet)
	}
	return error
}

// Intersect sets this filter's bitset to the intersection of the other filter's bitset.
func (f *Filter[T]) Intersect(other *Filter[T]) error {
	error := f.checkCompatibility(other)
	if error == nil {
		error = f.bitSet.Intersect(other.bitSet)
	}
	return error
}

// FPP returns the false positive probability rate given the number of elements in the filter.
func (f *Filter[T]) FPP(n uint64) float64 {
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
