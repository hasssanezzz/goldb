package internal

import (
	"bytes"
	"encoding/binary"
	"hash"
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	bitArray  []bool
	hashFuncs []hash.Hash64
}

// NewBloomFilter creates a new Bloom filter
// capacity: expected number of items
// falsePositiveRate: desired false positive probability (e.g., 0.01 for 1%)
func NewBloomFilter(capacity int, falsePositiveRate float64) *BloomFilter {
	// Calculate optimal bit array size
	bitSize := int(-float64(capacity) * math.Log(falsePositiveRate) / (math.Log(2) * math.Log(2)))

	// Calculate optimal number of hash functions
	hashCount := int(float64(bitSize) * math.Log(2) / float64(capacity))

	// Create bit array
	bitArray := make([]bool, bitSize)

	// Create hash functions
	var hashFuncs []hash.Hash64
	for range hashCount {
		hashFuncs = append(hashFuncs, fnv.New64())
	}

	return &BloomFilter{
		bitArray:  bitArray,
		hashFuncs: hashFuncs,
	}
}

// Add inserts an item into the Bloom filter
func (bf *BloomFilter) Add(item []byte) {
	for _, hashFunc := range bf.hashFuncs {
		hashFunc.Reset()
		hashFunc.Write(item)
		index := hashFunc.Sum64() % uint64(len(bf.bitArray))
		bf.bitArray[index] = true
	}
}

// Test checks if an item might be in the set
// Returns true if item might be present, false if definitely not present
func (bf *BloomFilter) Test(item []byte) bool {
	for _, hashFunc := range bf.hashFuncs {
		hashFunc.Reset()
		hashFunc.Write(item)
		hashValue := hashFunc.Sum64()

		index := hashValue % uint64(len(bf.bitArray))
		if !bf.bitArray[index] {
			return false // Definitely not in set
		}
	}
	return true // Might be in set
}

// ToBytes serializes the Bloom filter to bytes
func (bf *BloomFilter) ToBytes() []byte {
	var buf bytes.Buffer

	// Write number of hash functions
	binary.Write(&buf, binary.LittleEndian, uint32(len(bf.hashFuncs)))

	// Write bit array length
	binary.Write(&buf, binary.LittleEndian, uint32(len(bf.bitArray)))

	// Convert bit array to bytes
	bitArrayBytes := boolArrayToBytes(bf.bitArray)
	buf.Write(bitArrayBytes)

	return buf.Bytes()
}

// FromBytes deserializes a Bloom filter from bytes
func (bf *BloomFilter) FromBytes(data []byte) error {
	buf := bytes.NewReader(data)

	// Read number of hash functions
	var hashCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &hashCount); err != nil {
		return err
	}

	// Read bit array length
	var bitArrayLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &bitArrayLen); err != nil {
		return err
	}

	// Read bit array data
	bitArrayBytes := make([]byte, (bitArrayLen+7)/8)
	if _, err := buf.Read(bitArrayBytes); err != nil {
		return err
	}

	// Convert bytes back to bool array
	bitArray := bytesToBoolArray(bitArrayBytes, int(bitArrayLen))

	// Recreate hash functions
	var hashFuncs []hash.Hash64
	for i := 0; i < int(hashCount); i++ {
		hashFuncs = append(hashFuncs, fnv.New64())
	}

	// Update the Bloom filter
	bf.bitArray = bitArray
	bf.hashFuncs = hashFuncs

	return nil
}

// Helper function to convert bool array to bytes
func boolArrayToBytes(boolArray []bool) []byte {
	byteLen := (len(boolArray) + 7) / 8
	result := make([]byte, byteLen)

	for i, b := range boolArray {
		if b {
			byteIndex := i / 8
			bitIndex := i % 8
			result[byteIndex] |= 1 << bitIndex
		}
	}

	return result
}

// Helper function to convert bytes to bool array
func bytesToBoolArray(byteArray []byte, boolArrayLen int) []bool {
	result := make([]bool, boolArrayLen)

	for i := range boolArrayLen {
		byteIndex := i / 8
		bitIndex := i % 8
		if byteIndex < len(byteArray) {
			result[i] = (byteArray[byteIndex] & (1 << bitIndex)) != 0
		}
	}

	return result
}

// NewBloomFilterFromBytes creates a new Bloom filter from serialized bytes
func NewBloomFilterFromBytes(data []byte) (*BloomFilter, error) {
	bf := &BloomFilter{}
	if err := bf.FromBytes(data); err != nil {
		return nil, err
	}
	return bf, nil
}
