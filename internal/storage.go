package internal

import (
	"fmt"
	"io"
	"os"

	"github.com/hasssanezzz/goldb/shared"
)

// TODO: make this is a pair value manager not a storage manager, and create
// a seperate actuall storage manager which concerns itself with dealing
// with disk operations, the value manager will depend on the storage manager.

type StorageManager struct {
	writer   shared.WriteSeekCloser
	reader   io.ReadSeekCloser
	filename string
}

func NewStorageManager(filename string) (*StorageManager, error) {
	sm := &StorageManager{filename: filename}
	return sm, sm.Open()
}

func (s *StorageManager) Open() error {
	wfile, err := os.OpenFile(s.filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("storage manager can not open file for appending %q: %v", s.filename, err)
	}
	rfile, err := os.Open(s.filename)
	if err != nil {
		return fmt.Errorf("storage manager can not open file for reading %q: %v", s.filename, err)
	}
	s.writer = wfile
	s.reader = rfile
	return nil
}

func (s *StorageManager) WriteValue(value []byte) (uint32, error) {
	offset, err := s.writer.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("storage manager can not seek to end: %v", err)
	}

	_, err = s.writer.Write(value)
	if err != nil {
		return 0, fmt.Errorf("storage manager can not write value %q: %v", value, err)
	}
	return uint32(offset), err
}

// ReadValue read a value in frmo KV pair based on size and offset
func (s *StorageManager) ReadValue(indexNode IndexNode) ([]byte, error) {
	if indexNode.Size == 0 {
		return nil, &shared.ErrKeyNotFound{}
	}

	_, err := s.reader.Seek(int64(indexNode.Offset), io.SeekStart)
	if err != nil {
		return []byte{}, fmt.Errorf("storage manager can not read (%d, %d): %v", indexNode.Offset, indexNode.Size, err)
	}
	buf := make([]byte, indexNode.Size)
	_, err = s.reader.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// Compact deletes all unused values
func (s *StorageManager) Compact() error {
	panic("unimplemented")
}

func (s *StorageManager) Close() error {
	err := s.writer.Close()
	if err != nil {
		return err
	}
	err = s.reader.Close()
	return err
}
