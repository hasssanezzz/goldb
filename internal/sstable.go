package internal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/hasssanezzz/goldb/shared"
)

type TableMetadata struct {
	Path    string
	IsLevel bool
	Serial  uint32
	Size    uint32
	MinKey  string
	MaxKey  string
}

type SSTable struct {
	metadata TableMetadata
	config   *shared.EngineConfig
	file     io.ReadSeekCloser
}

func NewSSTable(metadata TableMetadata, config *shared.EngineConfig) (*SSTable, error) {
	table := &SSTable{
		config:   config,
		metadata: metadata,
	}

	if err := table.open(); err != nil {
		return nil, fmt.Errorf("failed to create logical SST: %v", err)
	}

	return table, nil
}

func (s *SSTable) Keys() ([]string, error) {
	results := make([]string, s.metadata.Size)

	pairSize := int(s.config.GetKVPairSize())
	if _, err := s.file.Seek(int64(s.config.GetMetadataSize()), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek at GetMetadatasize: %v", err)
	}

	buffer := make([]byte, pairSize*int(s.metadata.Size))
	if _, err := s.file.Read(buffer); err != nil {
		return nil, fmt.Errorf("failed to read from file: %v", err)
	}

	for i := 0; i < int(s.metadata.Size); i++ {
		keyStartIndex := i * pairSize
		keyEndIndex := keyStartIndex + shared.KeySize
		results[i] = shared.TrimPaddedKey(string(buffer[keyStartIndex:keyEndIndex]))
	}

	return results, nil
}

func (s *SSTable) Items() ([]KVPair, error) {
	results := make([]KVPair, s.metadata.Size)

	pairSize := s.config.GetKVPairSize()
	if _, err := s.file.Seek(int64(s.config.GetMetadataSize()), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek at GetMetadatasize: %v", err)
	}

	buffer := make([]byte, pairSize*s.metadata.Size)
	if _, err := s.file.Read(buffer); err != nil {
		return nil, fmt.Errorf("failed to read from file: %v", err)
	}

	for i := range s.metadata.Size {
		window := buffer[i*pairSize : (i*pairSize)+pairSize]
		key := window[:shared.KeySize]
		offset := binary.LittleEndian.Uint32(window[shared.KeySize : shared.KeySize+4])
		size := binary.LittleEndian.Uint32(window[shared.KeySize : shared.KeySize+4])

		results = append(results, KVPair{
			Key:   shared.TrimPaddedKey(string(key)),
			Value: Position{offset, size},
		})
	}

	return results, nil
}

func (s *SSTable) BSearch(key string) (Position, error) {
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

func (s *SSTable) Close() error {
	return s.file.Close()
}

func (s *SSTable) nthKey(n int) (KVPair, error) {
	position := int64(int(s.config.GetMetadataSize()) + n*int(s.config.GetKVPairSize()))
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
	file, err := os.Open(s.metadata.Path)
	if err != nil {
		return fmt.Errorf("can not open sstable %q: %v", s.metadata.Path, err)
	}
	s.file = file

	if err := s.metadata.Deserialize(file); err != nil {
		return fmt.Errorf("failed to open SST %q: %v", s.metadata.Path, err)
	}

	return nil
}
