package bloomfilter

import (
	"github.com/cespare/xxhash/v2"
	"github.com/hasssanezzz/goldb-engine/shared"
)

type BloomFilter struct {
	bitset  []bool
	size    uint64
	digests []*xxhash.Digest
}

func New() *BloomFilter {
	digests := make([]*xxhash.Digest, shared.HashFunctionsNumber)
	for i := 0; i < 7; i++ {
		digests[i] = xxhash.NewWithSeed(uint64(i))
	}

	return &BloomFilter{
		bitset:  make([]bool, shared.BloomFilterSize),
		size:    shared.BloomFilterSize,
		digests: digests,
	}
}

func (bf *BloomFilter) Add(key string) {
	for i := 0; i < 7; i++ {
		bf.digests[i].ResetWithSeed(uint64(i))
		bf.digests[i].Write([]byte(key))
		bf.bitset[bf.digests[i].Sum64()%bf.size] = true
	}
}

func (bf *BloomFilter) PossiblyContains(key string) bool {
	for i := 0; i < 7; i++ {
		bf.digests[i].ResetWithSeed(uint64(i))
		bf.digests[i].Write([]byte(key))
		if !bf.bitset[bf.digests[i].Sum64()%bf.size] {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Reset() {
	bf.bitset = make([]bool, bf.size)
}
