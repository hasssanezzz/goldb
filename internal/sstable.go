package internal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/hasssanezzz/goldb/shared"
)

type ReadWriteSeekCloser interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
}

type TableMetadata struct {
	Path       string
	IsLevel    bool
	Serial     uint32
	Size       uint32
	FilterSize uint32
	MinKey     string
	MaxKey     string
}

type SSTable struct {
	metadata TableMetadata
	config   *shared.EngineConfig
	bf       *BloomFilter
	file     ReadWriteSeekCloser
}

func NewSSTable(metadata TableMetadata, config *shared.EngineConfig) (*SSTable, error) {
	table := &SSTable{
		config:   config,
		metadata: metadata,
	}

	if err := table.open(); err != nil {
		return nil, fmt.Errorf("failed to open SST: %v", err)
	}

	return table, nil
}

func (s *SSTable) Keys() ([]string, error) {
	results := make([]string, 0, s.metadata.Size)

	pairSize := int(s.config.GetKVPairSize())
	if _, err := s.file.Seek(int64(s.config.GetMetadataSize())+int64(s.metadata.FilterSize), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek at GetMetadataSize+FilterSize: %v", err)
	}

	buffer := make([]byte, pairSize*int(s.metadata.Size))
	if _, err := s.file.Read(buffer); err != nil {
		return nil, fmt.Errorf("failed to read from file: %v", err)
	}

	for i := 0; i < int(s.metadata.Size); i++ {
		window := buffer[i*pairSize : (i*pairSize)+pairSize]
		key := window[:shared.KeySize]
		size := binary.LittleEndian.Uint32(window[shared.KeySize+4 : shared.KeySize+8])

		if size > 0 {
			results = append(results, shared.TrimPaddedKey(string(key)))
		}
	}

	return results, nil
}

func (s *SSTable) Items() ([]KVPair, error) {
	results := make([]KVPair, s.metadata.Size)

	pairSize := s.config.GetKVPairSize()
	if _, err := s.file.Seek(int64(s.config.GetMetadataSize())+int64(s.metadata.FilterSize), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek at GetMetadataSize+FilterSize: %v", err)
	}

	buffer := make([]byte, pairSize*s.metadata.Size)
	if _, err := s.file.Read(buffer); err != nil {
		return nil, fmt.Errorf("failed to read from file: %v", err)
	}

	for i := range s.metadata.Size {
		window := buffer[i*pairSize : (i*pairSize)+pairSize]
		key := window[:shared.KeySize]
		offset := binary.LittleEndian.Uint32(window[shared.KeySize : shared.KeySize+4])
		size := binary.LittleEndian.Uint32(window[shared.KeySize+4 : shared.KeySize+8])

		// TODO: should I skip deleted keys?

		results[i] = KVPair{
			Key:   shared.TrimPaddedKey(string(key)),
			Value: Position{offset, size},
		}
	}

	return results, nil
}

func (s *SSTable) Search(key string) (Position, error) {
	// Range & filter lookup
	if s.metadata.MinKey > key || s.metadata.MaxKey < key || !s.bf.Test(shared.KeyToBytes(key)) {
		return Position{}, &shared.ErrKeyNotFound{Key: key}
	}

	// Binary search
	left, right := 0, int(s.metadata.Size-1)
	for left <= right {
		mid := left + (right-left)/2
		pair, err := s.nthKey(mid)
		if err != nil {
			return Position{}, fmt.Errorf("sstable %q can not perform bsearch gettting the %dth key: %v", s.metadata.Path, mid, err)
		}

		if pair.Key < key {
			left = mid + 1
		} else if pair.Key > key {
			right = mid - 1
		} else {
			if pair.Value.Size == 0 {
				return Position{}, &shared.ErrKeyRemoved{Key: key}
			} else {
				return pair.Value, nil
			}
		}
	}

	return Position{}, &shared.ErrKeyNotFound{Key: key}
}

func (s *SSTable) Serialize(pairs []KVPair) error {
	// Create the filter
	s.bf = NewBloomFilter(int(s.metadata.Size), 0.01)

	// Feed the filter
	for _, pair := range pairs {
		s.bf.Add(shared.KeyToBytes(pair.Key))
	}
	filterBytes := s.bf.ToBytes()

	// Update the metadata with the filter's size
	s.metadata.FilterSize = uint32(len(filterBytes))

	// Write serialized metadata & filter bytes
	if _, err := s.file.Write(append(s.metadata.Serialize(), filterBytes...)); err != nil {
		return fmt.Errorf("SSTable[%d] failed to write metadata & filter: %v", s.metadata.Serial, err)
	}

	// Write the serialized pairs
	if _, err := s.file.Write(serializePairs(pairs)); err != nil {
		return fmt.Errorf("SSTable[%d] failed to write pairs of length %d: %v", s.metadata.Serial, len(pairs), err)
	}

	return nil
}

func (s *SSTable) Deserialize() error {
	// Read the metadata
	if err := s.metadata.Deserialize(s.file); err != nil {
		return fmt.Errorf("failed to open SST %q: %v", s.metadata.Path, err)
	}

	// Create a filter
	s.bf = NewBloomFilter(int(s.metadata.Size), 0.01)

	// Read the filter
	buf := make([]byte, s.metadata.FilterSize)
	if _, err := s.file.Read(buf); err != nil {
		return err
	}

	return s.bf.FromBytes(buf)
}

func (s *SSTable) Close() error {
	return s.file.Close()
}

func (s *SSTable) nthKey(n int) (KVPair, error) {
	position := int64(int(s.config.GetMetadataSize()) + int(s.metadata.FilterSize) + n*int(s.config.GetKVPairSize()))
	_, err := s.file.Seek(position, io.SeekStart)
	if err != nil {
		return KVPair{}, fmt.Errorf("sstable %q can not seek position %d: %v", s.metadata.Path, position, err)
	}

	keyBuffer := make([]byte, s.config.KeySize)
	numberBuffer := make([]byte, shared.UintSize)

	// read key string
	_, err = s.file.Read(keyBuffer)
	if err != nil {
		return KVPair{}, err
	}

	_, err = s.file.Read(numberBuffer)
	if err != nil {
		return KVPair{}, err
	}
	offset := binary.LittleEndian.Uint32(numberBuffer)

	_, err = s.file.Read(numberBuffer)
	if err != nil {
		return KVPair{}, err
	}
	size := binary.LittleEndian.Uint32(numberBuffer)

	return KVPair{
		Key: shared.TrimPaddedKey(string(keyBuffer)),
		Value: Position{
			Offset: offset,
			Size:   size,
		},
	}, nil
}

func (s *SSTable) open() error {
	file, err := os.OpenFile(s.metadata.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("can not open sstable %q: %v", s.metadata.Path, err)
	}
	s.file = file

	return nil
}

func serializeSSTable(metadata TableMetadata, config *shared.EngineConfig, pairs []KVPair) (*SSTable, error) {
	table, err := NewSSTable(metadata, config)
	if err != nil {
		return nil, fmt.Errorf("failed to open table %q: %v", metadata.Path, err)
	}

	if err := table.Serialize(pairs); err != nil {
		return nil, fmt.Errorf("failed to deserialize table %q: %v", metadata.Path, err)
	}

	return table, nil
}

func deserializeSSTable(metadata TableMetadata, config *shared.EngineConfig) (*SSTable, error) {
	table, err := NewSSTable(metadata, config)
	if err != nil {
		return nil, fmt.Errorf("failed to open table %q: %v", metadata.Path, err)
	}

	if err := table.Deserialize(); err != nil {
		return nil, fmt.Errorf("failed to deserialize table %q: %v", metadata.Path, err)
	}

	return table, nil
}
