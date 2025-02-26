package index_manager

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/hasssanezzz/goldb/internal/memtable"
	"github.com/hasssanezzz/goldb/internal/shared"
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
	table := &SSTable{config: config}
	table.metadata = metadata

	if err := table.open(); err != nil {
		return nil, err
	}

	return table, nil
}

func (s *SSTable) open() error {
	file, err := os.Open(s.metadata.Path)
	if err != nil {
		return fmt.Errorf("can not open sstable %q: %v", s.metadata.Path, err)
	}
	s.file = file
	s.ParseMetadata()
	return nil
}

func (s *SSTable) ParseMetadata() error {
	uintBuffer := make([]byte, shared.UintSize)
	keyBuffer := make([]byte, s.config.KeySize)

	// read isLevel
	isLevelBuffer := make([]byte, 1)
	_, err := s.file.Read(isLevelBuffer)
	if err != nil {
		return fmt.Errorf("can not read metadata from sstable %q: %v", s.metadata.Path, err)
	}
	s.metadata.IsLevel = isLevelBuffer[0] == 0xFF

	// read serial
	_, err = s.file.Read(uintBuffer)
	if err != nil {
		return fmt.Errorf("can not read metadata from sstable %q: %v", s.metadata.Path, err)
	}
	s.metadata.Serial = binary.LittleEndian.Uint32(uintBuffer)

	// read pair count
	_, err = s.file.Read(uintBuffer)
	if err != nil {
		return fmt.Errorf("can not read metadata from sstable %q: %v", s.metadata.Path, err)
	}
	s.metadata.Size = binary.LittleEndian.Uint32(uintBuffer)

	// read min key
	_, err = s.file.Read(keyBuffer)
	if err != nil {
		return fmt.Errorf("can not read metadata from sstable %q: %v", s.metadata.Path, err)
	}
	s.metadata.MinKey = shared.TrimPaddedKey(string(keyBuffer))

	// read max key
	_, err = s.file.Read(keyBuffer)
	if err != nil {
		return fmt.Errorf("can not read metadata from sstable %q: %v", s.metadata.Path, err)
	}
	s.metadata.MaxKey = shared.TrimPaddedKey(string(keyBuffer))

	return nil
}

func (s *SSTable) Keys() ([]string, error) {
	results := []string{}

	for i := 0; i < int(s.metadata.Size); i++ {
		pair, err := s.nthKey(i)
		if err != nil {
			return nil, fmt.Errorf("sstable seq scan can not read %dth key: %v", i, err)
		}
		results = append(results, pair.Key)
	}

	return results, nil
}

func (s *SSTable) KVPairs() ([]memtable.KVPair, error) {
	results := []memtable.KVPair{}

	for i := 0; i < int(s.metadata.Size); i++ {
		pair, err := s.nthKey(i)
		if err != nil {
			return nil, fmt.Errorf("sstable seq scan can not read %dth key: %v", i, err)
		}
		results = append(results, pair)
	}

	return results, nil
}

func (s *SSTable) BSearch(key string) (memtable.IndexNode, error) {
	left, right := 0, int(s.metadata.Size-1)
	for left <= right {
		mid := left + (right-left)/2
		pair, err := s.nthKey(mid)
		if err != nil {
			return memtable.IndexNode{}, fmt.Errorf("sstable %q can not perform bsearch gettting the %dth key: %v", s.metadata.Path, mid, err)
		}

		if pair.Key < key {
			left = mid + 1
		} else if pair.Key > key {
			right = mid - 1
		} else {
			if pair.Value.Size == 0 {
				return memtable.IndexNode{}, &shared.ErrKeyRemoved{Key: key}
			} else {
				return pair.Value, nil
			}
		}
	}

	return memtable.IndexNode{}, &shared.ErrKeyNotFound{Key: key}
}

func (s *SSTable) Close() error {
	return s.file.Close()
}

func (s *SSTable) nthKey(n int) (memtable.KVPair, error) {
	position := int64(int(s.config.GetMetadataSize()) + n*int(s.config.GetKVPairSize()))
	_, err := s.file.Seek(position, io.SeekStart)
	if err != nil {
		return memtable.KVPair{}, fmt.Errorf("sstable %q can not seek position %d: %v", s.metadata.Path, position, err)
	}

	keyBuffer := make([]byte, s.config.KeySize)
	numberBuffer := make([]byte, shared.UintSize)

	// read key string
	_, err = s.file.Read(keyBuffer)
	if err != nil {
		return memtable.KVPair{}, err
	}

	_, err = s.file.Read(numberBuffer)
	if err != nil {
		return memtable.KVPair{}, err
	}
	offset := binary.LittleEndian.Uint32(numberBuffer)

	_, err = s.file.Read(numberBuffer)
	if err != nil {
		return memtable.KVPair{}, err
	}
	size := binary.LittleEndian.Uint32(numberBuffer)

	return memtable.KVPair{
		Key: shared.TrimPaddedKey(string(keyBuffer)),
		Value: memtable.IndexNode{
			Offset: offset,
			Size:   size,
		},
	}, nil
}
