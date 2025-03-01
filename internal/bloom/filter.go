package bloom

import (
	"math"

	"github.com/spaolacci/murmur3"
)

type Filter struct {
	bitset []byte
	m      uint32
	k      uint32
}

// n is the number of elements expected to be added to the filter
// p is the Probability of false positives
func New(n uint, p float64) *Filter {
	m := uint32(math.Ceil(-(float64(n) * math.Log(p)) / math.Log(math.Pow(2.0, math.Log(2.0))))) // m is the length of the filter
	k := uint32(math.Ceil(math.Log(2.0) * float64(m) / float64(n)))                              // k is the number of hash functions

	return &Filter{
		bitset: make([]byte, (m+7)/8),
		m:      m,
		k:      k,
	}
}

func (f *Filter) hash(key string, seed uint32) uint32 {
	hasher := murmur3.New32WithSeed(seed)
	hasher.Write([]byte(key))
	return hasher.Sum32()
}

func (f *Filter) Add(key string) {
	for i := uint32(0); i < f.k; i++ {
		hash := f.hash(key, i)
		byteIndex := hash / 8
		bitIndex := hash % 8
		f.bitset[byteIndex] |= 1 << bitIndex
	}
}

func (f *Filter) PossiblyExists(key string) bool {
	for i := uint32(0); i < f.k; i++ {
		hash := f.hash(key, i)
		byteIndex := hash / 8
		bitIndex := hash % 8
		if f.bitset[byteIndex]&(1<<bitIndex) == 0 {
			return false
		}
	}
	return true
}
