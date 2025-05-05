package bloom

import (
	"math"

	"github.com/spaolacci/murmur3"
)

type Filter struct {
	Bitset []byte
	m      uint
	k      uint
}

// n is the number of elements expected to be added to the filter
// p is the Probability of false positives
func New(n uint, p float64, bitset []byte) *Filter {
	m := uint(math.Ceil(-(float64(n) * math.Log(p)) / math.Log(math.Pow(2.0, math.Log(2.0))))) // m is the length of the filter
	k := uint(math.Ceil(math.Log(2.0) * float64(m) / float64(n)))                              // k is the number of hash functions

	if bitset == nil {
		bitset = make([]byte, (m+7)/8)
	}

	return &Filter{
		Bitset: bitset,
		m:      m,
		k:      k,
	}
}

func (f *Filter) hash(key string, seed uint) uint32 {
	hasher := murmur3.New32WithSeed(uint32(seed))
	hasher.Write([]byte(key))
	return hasher.Sum32()
}

func (f *Filter) Add(key string) {
	for i := uint(0); i < f.k; i++ {
		hash := f.hash(key, i) % uint32(f.m)
		byteIndex := hash / 8
		bitIndex := hash % 8
		f.Bitset[byteIndex] |= 1 << bitIndex
	}
}

func (f *Filter) PossiblyExists(key string) bool {
	for i := uint(0); i < f.k; i++ {
		hash := f.hash(key, i) % uint32(f.m)
		byteIndex := hash / 8
		bitIndex := hash % 8
		if f.Bitset[byteIndex]&(1<<bitIndex) == 0 {
			return false
		}
	}
	return true
}
