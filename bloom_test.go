package bloom

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"math"
	mrand "math/rand"
	"testing"

	"github.com/zeebo/xxh3"
	// "testing/quick"
)

var XXH3 HashFunction[[]byte] = xxh3.HashSeed

func TestBasic(t *testing.T) {
	f, _ := NewXXH3Filter(1000, 4)

	// Rounded to nearest power of 2
	if f.bitSet.BitsCount() != 1000 {
		t.Errorf("should be sized to %v, got %v.", 1000, f.bitSet.BitsCount())
	}

	expectedBytes := int(math.Ceil(1000 / 8))
	if len(f.Bytes()) != expectedBytes {
		t.Errorf("should be sized to %v, got %v.", expectedBytes, len(f.Bytes()))
	}

	n1 := []byte("one")
	n2 := []byte("two")
	n3 := []byte("three")
	f.Add(n1)
	n3a := f.Test(n3)
	f.Add(n3)
	n1b := f.Test(n1)
	n2b := f.Test(n2)
	n3b := f.Test(n3)
	if !n1b {
		t.Errorf("%v should be in.", n1)
	}
	if n2b {
		t.Errorf("%v should not be in.", n2)
	}
	if n3a {
		t.Errorf("%v should not be in the first time we look.", n3)
	}
	if !n3b {
		t.Errorf("%v should be in the second time we look.", n3)
	}
}

func TestBasicUint32(t *testing.T) {
	f, _ := NewXXH3Filter(1000, 4)
	n1 := make([]byte, 4)
	n2 := make([]byte, 4)
	n3 := make([]byte, 4)
	n4 := make([]byte, 4)
	n5 := make([]byte, 4)
	binary.BigEndian.PutUint32(n1, 100)
	binary.BigEndian.PutUint32(n2, 101)
	binary.BigEndian.PutUint32(n3, 102)
	binary.BigEndian.PutUint32(n4, 103)
	binary.BigEndian.PutUint32(n5, 104)
	f.Add(n1)
	n3a := f.Test(n3)
	f.Add(n3)
	n1b := f.Test(n1)
	n2b := f.Test(n2)
	n3b := f.Test(n3)
	n5a := f.Test(n5)
	f.Add(n5)
	n5b := f.Test(n5)
	f.Test(n4)
	if !n1b {
		t.Errorf("%v should be in.", n1)
	}
	if n2b {
		t.Errorf("%v should not be in.", n2)
	}
	if n3a {
		t.Errorf("%v should not be in the first time we look.", n3)
	}
	if !n3b {
		t.Errorf("%v should be in the second time we look.", n3)
	}
	if n5a {
		t.Errorf("%v should not be in the first time we look.", n5)
	}
	if !n5b {
		t.Errorf("%v should be in the second time we look.", n5)
	}
}

func TestNewWithLowNumbers(t *testing.T) {
	f, _ := NewXXH3Filter(0, 0)
	if f.HashCount() != 1 {
		t.Errorf("%v should be 1", f.HashCount())
	}
	if f.BitCount() != 1 {
		t.Errorf("%v should be 1", f.BitCount())
	}

	f2, _ := NewXXH3Filter(2, 0)
	if f2.HashCount() != 1 {
		t.Errorf("%v should be 1", f2.HashCount())
	}
	if f2.BitCount() != 2 {
		t.Errorf("%v should be 1", f2.BitCount())
	}

	f3, _ := NewXXH3Filter(3, 0)
	if f3.HashCount() != 1 {
		t.Errorf("%v should be 1", f3.HashCount())
	}
	if f3.BitCount() != 3 {
		t.Errorf("%v should be 1", f3.BitCount())
	}
}

func TestHashCount(t *testing.T) {
	f, _ := NewXXH3Filter(1000, 4)
	if f.HashCount() != f.hashCount {
		t.Error("not accessing HashCount() correctly")
	}
}

func TestBitCount(t *testing.T) {
	f, _ := NewXXH3Filter(1000, 4)
	if f.BitCount() != f.bitCount {
		t.Error("not accessing BitCount() correctly")
	}
}

func TestBytes(t *testing.T) {
	b := make([]byte, 8)
	u := uint64(1)
	binary.BigEndian.PutUint64(b, u)

	f, _ := NewXXH3Filter(8, 1)
	expected := []byte{byte(0)}
	if !bytes.Equal(f.Bytes(), expected) {
		t.Errorf("expected Bytes() to be %v, got %v", expected, f.Bytes())
	}
}

func TestFPP(t *testing.T) {
	f, _ := NewXXH3FilterWithEstimates(1000, 0.001)

	for i := uint32(0); i < 1000; i++ {
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, i)
		f.Add(b)
	}
	count := 0

	for i := uint32(0); i < 1000; i++ {
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, i+1000)
		if f.Test(b) {
			count += 1
		}
	}
	if f.FPP(1000) > 0.001 {
		t.Errorf("Excessive FPP()! n=%v, m=%v, k=%v, fpp=%v", 1000, f.BitCount(), f.HashCount(), f.FPP(1000))
	}

	if EstimateFPP(1000000) != float64(1)/float64(1_000_000) {
		t.Errorf("EstimateFPP(1000000) should be 1/1000000")
	}

	v := EstimateFPP(0)
	if v != 0 {
		t.Errorf("EstimateFPP(0) should be 0, got %v", v)
	}
}

func TestUnion(t *testing.T) {
	f1, _ := NewXXH3FilterWithEstimates(20, 0.01)
	f1.Add([]byte{1})
	f2, _ := NewXXH3FilterWithEstimates(20, 0.01)
	f2.Add([]byte{2})
	err := f1.Union(f2)
	if err != nil {
		t.Errorf("should not return an error")
	}
	if !f1.Test([]byte{1}) {
		t.Errorf("should contain []byte{1}")
	}
	if !f1.Test([]byte{2}) {
		t.Errorf("should contain []byte{2}")
	}
}

func rangePopulate(filter *Filter[[]byte], from uint32, to uint32) {
	for i := from; i < to; i++ {
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, i)
		filter.Add(b)
	}
}

func TestUnionWithOverlap(t *testing.T) {
	f1, _ := NewXXH3FilterWithEstimates(20, 0.01)
	rangePopulate(f1, 1, 10)
	f2, _ := NewXXH3FilterWithEstimates(20, 0.01)
	rangePopulate(f1, 5, 15)
	err := f1.Union(f2)
	if err != nil {
		t.Errorf("should not return an error")
	}
	estimate := f1.EstimateEntries()
	if estimate < 12 || estimate > 17 {
		t.Errorf("entry count estimation out of range: %v", estimate)
	}
}

func TestIncompatibleBitCountUnion(t *testing.T) {
	f1, _ := NewXXH3Filter(128, 3)
	f1.Add([]byte{1})
	f2, _ := NewXXH3Filter(64, 3)
	f2.Add([]byte{2})
	err := f1.Union(f2)
	if err != ERR_INCOMPATIBLE_BIT_COUNT {
		t.Errorf("should return ERR_INCOMPATIBLE_BIT_COUNT")
	}
}

func TestIncompatibleHashCountUnion(t *testing.T) {
	f1, _ := NewXXH3Filter(128, 3)
	f1.Add([]byte{1})
	f2, _ := NewXXH3Filter(128, 2)
	f2.Add([]byte{2})
	err := f1.Union(f2)
	if err != ERR_INCOMPATIBLE_HASH_COUNT {
		t.Errorf("should return ERR_INCOMPATIBLE_HASH_COUNT")
	}
}

func modified_xxh3(value []byte, seed uint64) uint64 {
	return xxh3.HashSeed(value, seed+1)
}

func TestIncompatibleHashFunctiontUnion(t *testing.T) {

	f1, _ := NewFilter(128, 3, XXH3)
	f1.Add([]byte{1})
	f2, _ := NewFilter(128, 3, HashFunction[[]byte](modified_xxh3))
	f2.Add([]byte{2})
	err := f1.Union(f2)
	if err != ERR_INCOMPATIBLE_HASH_FUNCTIONS {
		t.Errorf("should return ERR_INCOMPATIBLE_HASH_FUNCTIONS")
	}
}

func TestCopy(t *testing.T) {
	f1, _ := NewXXH3FilterWithEstimates(20, 0.01)
	f1.Add([]byte{1})
	if !f1.Test([]byte{1}) {
		t.Errorf("should contain []byte{1}")
	}
	if f1.Test([]byte{2}) {
		t.Errorf("should not contain []byte{2}")
	}

	f2 := f1.Copy()
	f2.Add([]byte{2})
	if !f2.Test([]byte{1}) {
		t.Errorf("should contain []byte{1}")
	}
	if !f2.Test([]byte{2}) {
		t.Errorf("should contain []byte{2}")
	}

	if f1.Test([]byte{2}) {
		t.Errorf("should not contain []byte{2}")
	}
}

func TestIntersect(t *testing.T) {
	f1, _ := NewXXH3FilterWithEstimates(20, 0.01)
	f1.Add([]byte{1})
	f1.Add([]byte{2})
	f2, _ := NewXXH3FilterWithEstimates(20, 0.01)
	f2.Add([]byte{2})
	f2.Add([]byte{3})
	err := f1.Intersect(f2)
	if err != nil {
		t.Errorf("should not return an error")
	}
	if f1.Test([]byte{1}) {
		t.Errorf("should not contain []byte{1}")
	}
	if !f1.Test([]byte{2}) {
		t.Errorf("should contain []byte{2}")
	}
	if f1.Test([]byte{3}) {
		t.Errorf("should not contain []byte{3}")
	}
}

func TestIncompatibleBitCountIntersect(t *testing.T) {
	f1, _ := NewXXH3Filter(128, 3)
	f1.Add([]byte{1})
	f2, _ := NewXXH3Filter(64, 3)
	f2.Add([]byte{2})
	err := f1.Intersect(f2)
	if err != ERR_INCOMPATIBLE_BIT_COUNT {
		t.Errorf("should return ERR_INCOMPATIBLE_BIT_COUNT")
	}
}

func TestIncompatibleHashCountIntersect(t *testing.T) {
	f1, _ := NewXXH3Filter(128, 3)
	f1.Add([]byte{1})
	f2, _ := NewXXH3Filter(128, 2)
	f2.Add([]byte{2})
	err := f1.Intersect(f2)
	if err != ERR_INCOMPATIBLE_HASH_COUNT {
		t.Errorf("should return ERR_INCOMPATIBLE_HASH_COUNT")
	}
}

func TestIncompatibleHashFunctiontIntersect(t *testing.T) {

	f1, _ := NewFilter(128, 3, XXH3)
	f1.Add([]byte{1})
	f2, _ := NewFilter(128, 3, HashFunction[[]byte](modified_xxh3))
	f2.Add([]byte{2})
	err := f1.Intersect(f2)
	if err != ERR_INCOMPATIBLE_HASH_FUNCTIONS {
		t.Errorf("should return ERR_INCOMPATIBLE_HASH_FUNCTIONS")
	}
}

func BenchmarkEstimateFPP(b *testing.B) {
	for i := uint64(1); i < uint64(b.N); i++ {
		n := 1 / i
		NewXXH3FilterWithEstimates(n, EstimateFPP(n))
	}
}

func TestLargeNotPowerOfTwo(t *testing.T) {
	// Not a power of 2
	f, _ := NewXXH3Filter(9, 10)
	for i := 0; i < 8; i++ {
		item := make([]byte, 4)
		rand.Read(item)
		f.Add(item)
		if f.Test(item) != true {
			t.Errorf("should always return true for something added, i=%v, item=%v", i, item)
		}
	}
}

func TestEstimatedEntries(t *testing.T) {
	for i := 0; i < 20; i++ {
		f, _ := NewXXH3Filter(1024, 4)
		count := mrand.Intn(1024)
		for j := 0; j < count; j++ {
			item := make([]byte, 32)
			rand.Read(item)
			f.Add(item)
		}
		estimate := float64(f.EstimateEntries())
		fcount := float64(count)
		pct := math.Abs(100 * (estimate - fcount) / fcount)
		if pct > 20 {
			t.Errorf("Estimated count is off. Estimate: %f, actual: %v", estimate, count)
		}
	}
}

func TestEstimatedCapacity(t *testing.T) {
	for i := 0; i < 200; i++ {
		capacity := mrand.Intn(1024) * 100
		f, _ := NewXXH3FilterWithEstimates(uint64(capacity), EstimateFPP(uint64(capacity)))
		estimate := float64(f.EstimateCapacity())
		fcapacity := float64(capacity)
		pct := math.Abs(100 * (estimate - fcapacity) / fcapacity)
		if pct > 20 {
			t.Errorf("Estimated count is off. Estimate: %f, actual: %v", estimate, capacity)
		}
	}
}
